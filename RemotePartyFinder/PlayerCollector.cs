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

/// <summary>
/// ObjectTable을 주기적으로 스캔하여 플레이어 정보를 수집합니다.
/// 로컬 SQLite DB를 통해 중복/미전송 상태를 관리합니다.
/// </summary>
internal class PlayerCollector : IDisposable {
    private Plugin Plugin { get; }
    private PlayerLocalDatabase Database { get; }
    private HttpClient Client { get; } = new();
    private Stopwatch ScanTimer { get; } = new();
    private volatile bool _isUploading;

    private const int ScanIntervalSeconds = 5;

    // FFXIVClientStructs Character 구조체의 ContentId 오프셋 (0x2358)
    private const int ContentIdOffset = 0x2358;
    // FFXIVClientStructs Character 구조체의 AccountId 오프셋 (0x2350)
    private const int AccountIdOffset = 0x2350;

    internal PlayerCollector(Plugin plugin) {
        this.Plugin = plugin;
        this.Database = new PlayerLocalDatabase(plugin);
        this.ScanTimer.Start();
        this.Plugin.Framework.Update += this.OnUpdate;
    }

    public void Dispose() {
        this.Plugin.Framework.Update -= this.OnUpdate;
        this.Database.Dispose();
    }

    private void OnUpdate(IFramework framework) {
        if (this.ScanTimer.Elapsed < TimeSpan.FromSeconds(ScanIntervalSeconds)) {
            return;
        }
        this.ScanTimer.Restart();

        if (this._isUploading) {
            return;
        }

        var observedPlayers = new List<UploadablePlayer>();
        foreach (var obj in this.Plugin.ObjectTable) {
            if (obj.ObjectKind != ObjectKind.Player) {
                continue;
            }

            if (obj is not IPlayerCharacter playerCharacter) {
                continue;
            }

            var contentId = (ulong)Marshal.ReadInt64(obj.Address + ContentIdOffset);
            var accountId = (ulong)Marshal.ReadInt64(obj.Address + AccountIdOffset);
            var homeWorld = (ushort)playerCharacter.HomeWorld.RowId;
            var currentWorld = (ushort)playerCharacter.CurrentWorld.RowId;
            var name = obj.Name.TextValue;

            if (contentId == 0 || homeWorld == 0 || homeWorld >= 1000 || string.IsNullOrEmpty(name)) {
                continue;
            }

            observedPlayers.Add(new UploadablePlayer {
                ContentId = contentId,
                Name = name,
                HomeWorld = homeWorld,
                CurrentWorld = currentWorld,
                AccountId = accountId,
            });
        }

        if (observedPlayers.Count > 0) {
            this.Database.UpsertObservedPlayers(observedPlayers);
        }

        if (this.Database.PendingCount > 0) {
            this._isUploading = true;
            UploadFromDatabaseAsync();
        }
    }

    private void UploadFromDatabaseAsync() {
        Task.Run(async () => {
            try {
                var batch = this.Database.TakePendingBatch();
                if (batch.Count == 0) {
                    return;
                }

                var json = JsonConvert.SerializeObject(batch);
                var success = false;

                foreach (var uploadUrl in this.Plugin.Configuration.UploadUrls.Where(u => u.IsEnabled)) {
                    if (uploadUrl.FailureCount >= this.Plugin.Configuration.CircuitBreakerFailureThreshold) {
                        if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < this.Plugin.Configuration.CircuitBreakerBreakDurationMinutes) {
                            continue;
                        }
                    }

                    var baseUrl = uploadUrl.Url.TrimEnd('/');
                    if (baseUrl.EndsWith("/contribute/multiple", StringComparison.OrdinalIgnoreCase)) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
                    } else if (baseUrl.EndsWith("/contribute", StringComparison.OrdinalIgnoreCase)) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
                    }

                    var playersUrl = baseUrl + "/contribute/players";

                    try {
                        using var request = IngestRequestFactory.CreatePostJsonRequest(
                            this.Plugin.Configuration,
                            playersUrl,
                            "/contribute/players",
                            json
                        );
                        var resp = await this.Client.SendAsync(request);

                        if (resp.IsSuccessStatusCode) {
                            success = true;
                            uploadUrl.FailureCount = 0;
                        } else {
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                            if ((int)resp.StatusCode == 429) {
                                var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(resp);
                                if (retryAfter.HasValue) {
                                    Plugin.Log.Warning($"PlayerCollector: server rate limited {playersUrl}, retry_after={retryAfter.Value}s");
                                }
                            }
                        }

                        Plugin.Log.Debug($"PlayerCollector: {playersUrl}: {resp.StatusCode} ({batch.Count} players)");
                    } catch (Exception ex) {
                        uploadUrl.FailureCount++;
                        uploadUrl.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"PlayerCollector upload error to {playersUrl}: {ex.Message}");
                    }
                }

                if (success) {
                    this.Database.MarkBatchUploaded(batch);
                } else {
                    Plugin.Log.Warning($"PlayerCollector: Upload failed, {batch.Count} players remain pending. Total pending: {this.Database.PendingCount}");
                }
            } catch (Exception e) {
                Plugin.Log.Error($"PlayerCollector upload error: {e.Message}");
            } finally {
                this._isUploading = false;
            }
        });
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal class UploadablePlayer {
    public ulong ContentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }
    [JsonProperty(DefaultValueHandling = DefaultValueHandling.Include)]
    public ushort CurrentWorld { get; set; }
    public ulong AccountId { get; set; }
}
