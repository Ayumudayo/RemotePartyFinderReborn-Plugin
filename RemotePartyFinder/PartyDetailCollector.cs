using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

internal sealed class PartyDetailCollector : IDisposable {
    private static readonly TimeSpan ScanInterval = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan RequeueSameFingerprintDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan UploadPumpInterval = TimeSpan.FromMilliseconds(150);
    private const int MaxRetryCount = 12;

    private readonly Plugin _plugin;
    private readonly HttpClient _httpClient = new();
    private readonly Stopwatch _scanTimer = new();
    private readonly Stopwatch _uploadPumpTimer = new();
    private readonly ConcurrentDictionary<uint, PendingDetailEntry> _pendingDetails = new();

    private uint _lastQueuedListingId;
    private ulong _lastQueuedFingerprint;
    private DateTime _lastQueuedAtUtc = DateTime.MinValue;

    private volatile uint _lastSuccessfulUploadListingId;
    private long _lastSuccessfulUploadAtUtcTicks;
    private long _lastSuccessfulUploadAckVersion;

    private volatile uint _lastTerminalUploadListingId;
    private long _lastTerminalUploadAtUtcTicks;
    private long _lastTerminalUploadAckVersion;

    private volatile bool _uploadWorkerBusy;

    internal uint LastUploadedListingId => _lastQueuedListingId;
    internal uint LastSuccessfulUploadListingId => _lastSuccessfulUploadListingId;
    internal DateTime LastSuccessfulUploadAtUtc => new(Interlocked.Read(ref _lastSuccessfulUploadAtUtcTicks), DateTimeKind.Utc);
    internal long LastSuccessfulUploadAckVersion => Interlocked.Read(ref _lastSuccessfulUploadAckVersion);
    internal uint LastTerminalUploadListingId => _lastTerminalUploadListingId;
    internal DateTime LastTerminalUploadAtUtc => new(Interlocked.Read(ref _lastTerminalUploadAtUtcTicks), DateTimeKind.Utc);
    internal long LastTerminalUploadAckVersion => Interlocked.Read(ref _lastTerminalUploadAckVersion);
    internal int PendingQueueCount => _pendingDetails.Count;

    internal PartyDetailCollector(Plugin plugin) {
        _plugin = plugin;
        _scanTimer.Start();
        _uploadPumpTimer.Start();
        _plugin.Framework.Update += OnUpdate;
    }

    public void Dispose() {
        _plugin.Framework.Update -= OnUpdate;
    }

    private unsafe void OnUpdate(IFramework framework) {
        if (_scanTimer.Elapsed < ScanInterval) {
            return;
        }

        _scanTimer.Restart();
        PumpUploadQueue();

        var addonPtr = _plugin.GameGui.GetAddonByName("LookingForGroupDetail", 1);
        if (addonPtr == 0) {
            _lastQueuedListingId = 0;
            _lastQueuedFingerprint = 0;
            return;
        }

        var lookingForGroupAgent = AgentLookingForGroup.Instance();
        if (lookingForGroupAgent == null) {
            return;
        }

        ref var viewedListing = ref lookingForGroupAgent->LastViewedListing;
        if (viewedListing.ListingId == 0) {
            return;
        }

        var leaderContentId = viewedListing.LeaderContentId;
        var homeWorld = viewedListing.HomeWorld;
        if (leaderContentId == 0 || homeWorld == 0 || homeWorld >= 1000) {
            return;
        }

        var effectiveParties = Math.Max(1, (int)viewedListing.NumberOfParties);
        var declaredSlots = Math.Max((int)viewedListing.TotalSlots, effectiveParties * 8);
        var slotCount = Math.Clamp(declaredSlots, 0, 48);
        if (slotCount <= 0) {
            return;
        }

        var memberContentIds = new List<ulong>(slotCount);
        var memberJobs = new List<byte>(slotCount);
        var nonZeroMemberCount = 0;

        for (var slotIndex = 0; slotIndex < slotCount; slotIndex++) {
            var memberContentId = viewedListing.MemberContentIds[slotIndex];
            memberContentIds.Add(memberContentId);
            memberJobs.Add(viewedListing.Jobs[slotIndex]);
            if (memberContentId != 0) {
                nonZeroMemberCount++;
            }
        }

        if (nonZeroMemberCount == 0) {
            return;
        }

        var payload = new UploadablePartyDetail {
            ListingId = viewedListing.ListingId,
            LeaderContentId = leaderContentId,
            LeaderName = lookingForGroupAgent->LastLeader.ToString(),
            HomeWorld = homeWorld,
            MemberContentIds = memberContentIds,
            MemberJobs = memberJobs,
        };

        var fingerprint = ComputeFingerprint(memberContentIds, memberJobs);

        var now = DateTime.UtcNow;
        if (payload.ListingId == _lastQueuedListingId
            && fingerprint == _lastQueuedFingerprint
            && now - _lastQueuedAtUtc < RequeueSameFingerprintDelay) {
            return;
        }

        Plugin.Log.Debug(
            $"PartyDetailCollector: Uploading detail listing={payload.ListingId} leader={payload.LeaderContentId} world={payload.HomeWorld} members={payload.MemberContentIds.Count} non_zero_members={nonZeroMemberCount} parties={effectiveParties} fingerprint={fingerprint}");

        Enqueue(payload, fingerprint, now);
        _lastQueuedListingId = payload.ListingId;
        _lastQueuedFingerprint = fingerprint;
        _lastQueuedAtUtc = now;
    }

    private void Enqueue(UploadablePartyDetail payload, ulong fingerprint, DateTime nowUtc) {
        var queued = new PendingDetailEntry(
            payload,
            fingerprint,
            nowUtc,
            0,
            nowUtc
        );

        _pendingDetails.AddOrUpdate(
            payload.ListingId,
            _ => queued,
            (_, existing) => existing.Fingerprint == fingerprint ? existing : queued
        );
    }

    private void PumpUploadQueue() {
        if (_uploadWorkerBusy) {
            return;
        }

        if (_uploadPumpTimer.Elapsed < UploadPumpInterval) {
            return;
        }

        var now = DateTime.UtcNow;
        var next = _pendingDetails.Values
            .Where(candidate => candidate.NextAttemptAtUtc <= now)
            .OrderBy(candidate => candidate.NextAttemptAtUtc)
            .ThenBy(candidate => candidate.QueuedAtUtc)
            .FirstOrDefault();

        if (next == null) {
            return;
        }

        _uploadWorkerBusy = true;
        _uploadPumpTimer.Restart();
        _ = ProcessQueuedUploadAsync(next);
    }

    private Task ProcessQueuedUploadAsync(PendingDetailEntry pending) {
        return Task.Run(async () => {
            try {
                var uploadResult = await TryUploadToAllTargetsAsync(pending.Detail);
                switch (uploadResult) {
                    case DetailUploadResult.Applied:
                        RemoveIfUnchanged(pending);
                        var successAt = DateTime.UtcNow;
                        Interlocked.Exchange(ref _lastSuccessfulUploadAtUtcTicks, successAt.Ticks);
                        _lastSuccessfulUploadListingId = pending.Detail.ListingId;
                        Interlocked.Increment(ref _lastSuccessfulUploadAckVersion);
                        break;

                    case DetailUploadResult.ListingMissing:
                        RemoveIfUnchanged(pending);
                        var missingAt = DateTime.UtcNow;
                        Interlocked.Exchange(ref _lastTerminalUploadAtUtcTicks, missingAt.Ticks);
                        _lastTerminalUploadListingId = pending.Detail.ListingId;
                        Interlocked.Increment(ref _lastTerminalUploadAckVersion);
                        Plugin.Log.Debug($"PartyDetailCollector: listing {pending.Detail.ListingId} missing on server while applying detail update; dropping queued detail payload.");
                        break;

                    default:
                        ScheduleRetry(pending);
                        break;
                }
            } catch (Exception exception) {
                Plugin.Log.Error($"PartyDetailCollector upload error: {exception.Message}");
                ScheduleRetry(pending);
            } finally {
                _uploadWorkerBusy = false;
            }
        });
    }

    private async Task<DetailUploadResult> TryUploadToAllTargetsAsync(UploadablePartyDetail payload) {
        var jsonPayload = JsonConvert.SerializeObject(payload);
        var anyApplied = false;
        var anyMissing = false;

        foreach (var uploadTarget in _plugin.Configuration.UploadUrls.Where(static candidate => candidate.IsEnabled)) {
            if (IsCircuitOpen(uploadTarget)) {
                continue;
            }

            var endpointUrl = BuildDetailEndpoint(uploadTarget.Url);
            try {
                using var request = IngestRequestFactory.CreatePostJsonRequest(
                    _plugin.Configuration,
                    endpointUrl,
                    "/contribute/detail",
                    jsonPayload
                );
                var response = await _httpClient.SendAsync(request);
                var body = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode) {
                    uploadTarget.FailureCount = 0;

                    if (TryParseDetailResponse(body, out var matchedCount, out var modifiedCount)) {
                        if (matchedCount == 0) {
                            anyMissing = true;
                        } else {
                            anyApplied = true;
                        }

                        Plugin.Log.Debug($"PartyDetailCollector: {endpointUrl}: {response.StatusCode} matched={matchedCount} modified={modifiedCount}");
                    } else {
                        anyApplied = true;
                        Plugin.Log.Debug($"PartyDetailCollector: {endpointUrl}: {response.StatusCode} {body}");
                    }
                } else {
                    uploadTarget.FailureCount++;
                    uploadTarget.LastFailureTime = DateTime.UtcNow;
                    if ((int)response.StatusCode == 429) {
                        var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(response);
                        if (retryAfter.HasValue) {
                            Plugin.Log.Warning($"PartyDetailCollector: rate limited by {endpointUrl}, retry_after={retryAfter.Value}s");
                        }
                    }

                    Plugin.Log.Debug($"PartyDetailCollector: {endpointUrl}: {response.StatusCode} {body}");
                }
            } catch (Exception exception) {
                uploadTarget.FailureCount++;
                uploadTarget.LastFailureTime = DateTime.UtcNow;
                Plugin.Log.Error($"PartyDetailCollector upload error to {endpointUrl}: {exception.Message}");
            }
        }

        if (anyApplied) {
            return DetailUploadResult.Applied;
        }

        if (anyMissing) {
            return DetailUploadResult.ListingMissing;
        }

        return DetailUploadResult.RetryableFailure;
    }

    private bool IsCircuitOpen(UploadUrl uploadTarget) {
        if (uploadTarget.FailureCount < _plugin.Configuration.CircuitBreakerFailureThreshold) {
            return false;
        }

        var elapsedSinceLastFailure = DateTime.UtcNow - uploadTarget.LastFailureTime;
        return elapsedSinceLastFailure.TotalMinutes < _plugin.Configuration.CircuitBreakerBreakDurationMinutes;
    }

    private static string BuildDetailEndpoint(string configuredUrl) {
        var baseUrl = configuredUrl.TrimEnd('/');
        if (baseUrl.EndsWith("/contribute/multiple")) {
            baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
        } else if (baseUrl.EndsWith("/contribute")) {
            baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
        }

        return baseUrl + "/contribute/detail";
    }

    private static bool TryParseDetailResponse(string rawBody, out long matchedCount, out long modifiedCount) {
        matchedCount = 0;
        modifiedCount = 0;

        var body = rawBody.Trim();
        if (!body.StartsWith('{')) {
            return false;
        }

        try {
            var parsed = JsonConvert.DeserializeObject<ContributeDetailResponse>(body);
            if (parsed == null) {
                return false;
            }

            matchedCount = parsed.MatchedCount;
            modifiedCount = parsed.ModifiedCount;
            return true;
        } catch {
            return false;
        }
    }

    private void RemoveIfUnchanged(PendingDetailEntry pending) {
        if (!_pendingDetails.TryGetValue(pending.Detail.ListingId, out var current)) {
            return;
        }

        if (current.Fingerprint != pending.Fingerprint) {
            return;
        }

        _pendingDetails.TryRemove(pending.Detail.ListingId, out _);
    }

    private void ScheduleRetry(PendingDetailEntry pending) {
        if (!_pendingDetails.TryGetValue(pending.Detail.ListingId, out var current)) {
            return;
        }

        if (current.Fingerprint != pending.Fingerprint) {
            return;
        }

        if (current.AttemptCount >= MaxRetryCount) {
            _pendingDetails.TryRemove(pending.Detail.ListingId, out _);
            Plugin.Log.Warning($"PartyDetailCollector: dropping detail payload listing={pending.Detail.ListingId} after {current.AttemptCount} retries");
            return;
        }

        var nextAttemptCount = current.AttemptCount + 1;
        var retryDelayMs = ComputeRetryDelayMs(nextAttemptCount);
        var updated = current with {
            AttemptCount = nextAttemptCount,
            NextAttemptAtUtc = DateTime.UtcNow.AddMilliseconds(retryDelayMs),
        };

        _pendingDetails.TryUpdate(pending.Detail.ListingId, updated, current);
    }

    private static int ComputeRetryDelayMs(int attemptCount) {
        var exponent = Math.Min(attemptCount, 5);
        var baseDelay = 1000 * (1 << exponent);
        var boundedDelay = Math.Min(baseDelay, 15000);
        var jitterMs = Random.Shared.Next(50, 250);
        return boundedDelay + jitterMs;
    }

    private static ulong ComputeFingerprint(List<ulong> memberContentIds, List<byte> memberJobs) {
        unchecked {
            var hash = 1469598103934665603UL;
            hash = MixFnv(hash, (ulong)memberContentIds.Count);
            for (var i = 0; i < memberContentIds.Count; i++) {
                hash = MixFnv(hash, memberContentIds[i]);
                hash = MixFnv(hash, memberJobs[i]);
            }

            return hash;
        }
    }

    private static ulong MixFnv(ulong hash, ulong value) {
        unchecked {
            hash ^= value;
            hash *= 1099511628211UL;
            return hash;
        }
    }

    private sealed record PendingDetailEntry(
        UploadablePartyDetail Detail,
        ulong Fingerprint,
        DateTime QueuedAtUtc,
        int AttemptCount,
        DateTime NextAttemptAtUtc
    );

    private sealed record ContributeDetailResponse {
        [JsonProperty("matched_count")]
        public long MatchedCount { get; init; }

        [JsonProperty("modified_count")]
        public long ModifiedCount { get; init; }
    }

    private enum DetailUploadResult {
        Applied,
        ListingMissing,
        RetryableFailure,
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class UploadablePartyDetail {
    public uint ListingId { get; set; }
    public ulong LeaderContentId { get; set; }
    public string LeaderName { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }
    public List<ulong> MemberContentIds { get; set; } = new();
    public List<byte> MemberJobs { get; set; } = new();
}
