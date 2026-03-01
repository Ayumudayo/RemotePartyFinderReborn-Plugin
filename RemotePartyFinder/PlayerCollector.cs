using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Dalamud.Game.ClientState.Objects.Enums;
using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

internal sealed class PlayerCollector : IDisposable {
    private const int ScanIntervalSeconds = 5;
    private const int ContentIdOffset = 0x2358;
    private const int AccountIdOffset = 0x2350;

    private readonly Plugin _plugin;
    private readonly PlayerLocalDatabase _database;
    private readonly HttpClient _httpClient = new();
    private readonly Stopwatch _scanTimer = new();

    private volatile bool _uploadInProgress;

    internal int PendingCount => _database.PendingCount;
    internal bool IsUploadInProgress => _uploadInProgress;

    internal PlayerCollector(Plugin plugin) {
        _plugin = plugin;
        _database = new PlayerLocalDatabase(plugin);
        _scanTimer.Start();
        _plugin.Framework.Update += OnUpdate;
    }

    public void Dispose() {
        _plugin.Framework.Update -= OnUpdate;
        _database.Dispose();
    }

    internal void TriggerManualUploadNow() {
        if (_uploadInProgress) {
            Plugin.Log.Warning($"PlayerCollector: Manual upload requested while upload is already in progress. Pending={_database.PendingCount}");
            return;
        }

        _uploadInProgress = true;
        Plugin.Log.Information($"PlayerCollector: Manual upload requested. Pending={_database.PendingCount}");
        _ = UploadPendingBatchAsync();
    }

    internal int TriggerManualFullResyncUploadNow() {
        var requeued = _database.MarkAllPlayersDirty();
        Plugin.Log.Information($"PlayerCollector: Manual full player-cache resync requested. Requeued={requeued} Pending={_database.PendingCount}");
        TriggerManualUploadNow();
        return requeued;
    }

    private void OnUpdate(IFramework framework) {
        if (_scanTimer.Elapsed < TimeSpan.FromSeconds(ScanIntervalSeconds)) {
            return;
        }

        _scanTimer.Restart();
        if (_uploadInProgress) {
            return;
        }

        var observedPlayers = CollectObservedPlayers();
        if (observedPlayers.Count > 0) {
            _database.UpsertObservedPlayers(observedPlayers);
        }

        if (_database.PendingCount <= 0) {
            return;
        }

        _uploadInProgress = true;
        _ = UploadPendingBatchAsync();
    }

    private List<UploadablePlayer> CollectObservedPlayers() {
        var players = new List<UploadablePlayer>();
        foreach (var gameObject in _plugin.ObjectTable) {
            if (gameObject.ObjectKind != ObjectKind.Player) {
                continue;
            }

            if (gameObject is not IPlayerCharacter character) {
                continue;
            }

            var contentId = (ulong)Marshal.ReadInt64(gameObject.Address + ContentIdOffset);
            var accountId = (ulong)Marshal.ReadInt64(gameObject.Address + AccountIdOffset);
            var homeWorld = (ushort)character.HomeWorld.RowId;
            var currentWorld = (ushort)character.CurrentWorld.RowId;
            var name = gameObject.Name.TextValue;

            if (contentId == 0 || homeWorld == 0 || homeWorld >= 1000 || string.IsNullOrEmpty(name)) {
                continue;
            }

            players.Add(new UploadablePlayer {
                ContentId = contentId,
                Name = name,
                HomeWorld = homeWorld,
                CurrentWorld = currentWorld,
                AccountId = accountId,
            });
        }

        return players;
    }

    private Task UploadPendingBatchAsync() {
        return Task.Run(async () => {
            try {
                var batch = _database.TakePendingBatch();
                if (batch.Count == 0) {
                    return;
                }

                var jsonPayload = JsonConvert.SerializeObject(batch);
                var anySuccess = false;

                foreach (var uploadTarget in _plugin.Configuration.UploadUrls.Where(static candidate => candidate.IsEnabled)) {
                    if (IsCircuitOpen(uploadTarget)) {
                        continue;
                    }

                    var endpointUrl = BuildPlayersEndpoint(uploadTarget.Url);
                    try {
                        using var request = IngestRequestFactory.CreatePostJsonRequest(
                            _plugin.Configuration,
                            endpointUrl,
                            "/contribute/players",
                            jsonPayload
                        );
                        var response = await _httpClient.SendAsync(request);

                        if (response.IsSuccessStatusCode) {
                            anySuccess = true;
                            uploadTarget.FailureCount = 0;
                        } else {
                            uploadTarget.FailureCount++;
                            uploadTarget.LastFailureTime = DateTime.UtcNow;
                            if ((int)response.StatusCode == 429) {
                                var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(response);
                                if (retryAfter.HasValue) {
                                    Plugin.Log.Warning($"PlayerCollector: server rate limited {endpointUrl}, retry_after={retryAfter.Value}s");
                                }
                            }
                        }

                        Plugin.Log.Debug($"PlayerCollector: {endpointUrl}: {response.StatusCode} ({batch.Count} players)");
                    } catch (Exception exception) {
                        uploadTarget.FailureCount++;
                        uploadTarget.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"PlayerCollector upload error to {endpointUrl}: {exception.Message}");
                    }
                }

                if (anySuccess) {
                    _database.MarkBatchUploaded(batch);
                } else {
                    Plugin.Log.Warning($"PlayerCollector: Upload failed, {batch.Count} players remain pending. Total pending: {_database.PendingCount}");
                }
            } catch (Exception exception) {
                Plugin.Log.Error($"PlayerCollector upload error: {exception.Message}");
            } finally {
                _uploadInProgress = false;
            }
        });
    }

    private bool IsCircuitOpen(UploadUrl uploadTarget) {
        if (uploadTarget.FailureCount < _plugin.Configuration.CircuitBreakerFailureThreshold) {
            return false;
        }

        var elapsedSinceFailure = DateTime.UtcNow - uploadTarget.LastFailureTime;
        return elapsedSinceFailure.TotalMinutes < _plugin.Configuration.CircuitBreakerBreakDurationMinutes;
    }

    private static string BuildPlayersEndpoint(string configuredUrl) {
        var baseUrl = configuredUrl.TrimEnd('/');
        if (baseUrl.EndsWith("/contribute/multiple", StringComparison.OrdinalIgnoreCase)) {
            baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
        } else if (baseUrl.EndsWith("/contribute", StringComparison.OrdinalIgnoreCase)) {
            baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
        }

        return baseUrl + "/contribute/players";
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class UploadablePlayer {
    public ulong ContentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }

    [JsonProperty(DefaultValueHandling = DefaultValueHandling.Include)]
    public ushort CurrentWorld { get; set; }

    public ulong AccountId { get; set; }
}
