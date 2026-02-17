using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

/// <summary>
/// AgentLookingForGroup.Detailed에서 PF 디테일 정보를 수집합니다.
/// </summary>
internal class PartyDetailCollector : IDisposable {
    private Plugin Plugin { get; }
    private HttpClient Client { get; } = new();
    private System.Diagnostics.Stopwatch ScanTimer { get; } = new();
    private System.Diagnostics.Stopwatch UploadTimer { get; } = new();
    private ConcurrentDictionary<uint, QueuedDetailPayload> PendingDetails { get; } = new();

    private uint lastQueuedListingId = 0;
    private ulong lastQueuedFingerprint = 0;
    private DateTime lastQueuedAtUtc = DateTime.MinValue;

    private volatile uint lastSuccessfulUploadListingId = 0;
    private long lastSuccessfulUploadAtUtcTicks = 0;
    private long lastSuccessfulUploadAckVersion = 0;

    private volatile uint lastTerminalUploadListingId = 0;
    private long lastTerminalUploadAtUtcTicks = 0;
    private long lastTerminalUploadAckVersion = 0;

    private volatile bool isUploadWorkerBusy;

    private static readonly TimeSpan RetrySameFingerprintInterval = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan UploadPumpInterval = TimeSpan.FromMilliseconds(150);
    private const int MaxQueueRetryCount = 12;

    internal uint LastUploadedListingId => this.lastQueuedListingId;
    internal uint LastSuccessfulUploadListingId => this.lastSuccessfulUploadListingId;
    internal DateTime LastSuccessfulUploadAtUtc => new(Interlocked.Read(ref this.lastSuccessfulUploadAtUtcTicks), DateTimeKind.Utc);
    internal long LastSuccessfulUploadAckVersion => Interlocked.Read(ref this.lastSuccessfulUploadAckVersion);
    internal uint LastTerminalUploadListingId => this.lastTerminalUploadListingId;
    internal DateTime LastTerminalUploadAtUtc => new(Interlocked.Read(ref this.lastTerminalUploadAtUtcTicks), DateTimeKind.Utc);
    internal long LastTerminalUploadAckVersion => Interlocked.Read(ref this.lastTerminalUploadAckVersion);
    internal int PendingQueueCount => this.PendingDetails.Count;

    internal PartyDetailCollector(Plugin plugin) {
        this.Plugin = plugin;
        this.ScanTimer.Start();
        this.UploadTimer.Start();
        this.Plugin.Framework.Update += this.OnUpdate;
    }

    public void Dispose() {
        this.Plugin.Framework.Update -= this.OnUpdate;
    }

    private unsafe void OnUpdate(IFramework framework) {
        // 성능 최적화: 200ms마다 체크
        if (this.ScanTimer.ElapsedMilliseconds < 200) return;
        this.ScanTimer.Restart();

        this.PumpUploadQueue();

        // UI 창(Addon)이 열려있는지 확인
        nint addonPtr = this.Plugin.GameGui.GetAddonByName("LookingForGroupDetail", 1);
        if (addonPtr == 0) {
            this.lastQueuedListingId = 0;
            this.lastQueuedFingerprint = 0;
            return;
        }

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) return;

        ref var detailed = ref agent->LastViewedListing;
        if (detailed.ListingId == 0) return;

        var leaderContentId = detailed.LeaderContentId;
        var homeWorld = detailed.HomeWorld;
        var leaderName = agent->LastLeader.ToString();

        // 유효성 검사
        if (leaderContentId == 0 || homeWorld == 0 || homeWorld >= 1000) return;

        var effectiveParties = Math.Max(1, (int)detailed.NumberOfParties);
        var declaredSlots = Math.Max((int)detailed.TotalSlots, effectiveParties * 8);
        var totalSlots = Math.Clamp(declaredSlots, 0, 48);
        if (totalSlots <= 0) return;

        var memberContentIds = new List<ulong>(totalSlots);
        var memberJobs = new List<byte>(totalSlots);

        for (var i = 0; i < totalSlots; i++) {
            memberContentIds.Add(detailed.MemberContentIds[i]);
            memberJobs.Add(detailed.Jobs[i]);
        }

        var nonZeroMembers = memberContentIds.Count(contentId => contentId != 0);
        if (nonZeroMembers == 0) {
            return;
        }

        var fingerprint = ComputeMemberFingerprint(memberContentIds, memberJobs);
        if (detailed.ListingId == this.lastQueuedListingId
            && fingerprint == this.lastQueuedFingerprint
            && DateTime.UtcNow - this.lastQueuedAtUtc < RetrySameFingerprintInterval) {
            return;
        }

        var uploadData = new UploadablePartyDetail {
            ListingId = detailed.ListingId,
            LeaderContentId = leaderContentId,
            LeaderName = leaderName,
            HomeWorld = homeWorld,
            MemberContentIds = memberContentIds,
            MemberJobs = memberJobs,
        };

        Plugin.Log.Debug(
            $"PartyDetailCollector: Uploading detail listing={uploadData.ListingId} leader={uploadData.LeaderContentId} world={uploadData.HomeWorld} members={uploadData.MemberContentIds.Count} non_zero_members={nonZeroMembers} parties={effectiveParties} fingerprint={fingerprint}");

        this.EnqueueDetail(uploadData, fingerprint);
        this.lastQueuedListingId = detailed.ListingId;
        this.lastQueuedFingerprint = fingerprint;
        this.lastQueuedAtUtc = DateTime.UtcNow;
    }

    private void EnqueueDetail(UploadablePartyDetail detail, ulong fingerprint) {
        var queuedAt = DateTime.UtcNow;
        var payload = new QueuedDetailPayload(
            detail,
            fingerprint,
            queuedAt,
            0,
            queuedAt
        );

        this.PendingDetails.AddOrUpdate(
            detail.ListingId,
            _ => payload,
            (_, existing) => existing.Fingerprint == fingerprint ? existing : payload
        );
    }

    private void PumpUploadQueue() {
        if (this.isUploadWorkerBusy) {
            return;
        }

        if (this.UploadTimer.Elapsed < UploadPumpInterval) {
            return;
        }

        var now = DateTime.UtcNow;
        var nextPayload = this.PendingDetails.Values
            .Where(payload => payload.NextAttemptAtUtc <= now)
            .OrderBy(payload => payload.NextAttemptAtUtc)
            .ThenBy(payload => payload.QueuedAtUtc)
            .FirstOrDefault();

        if (nextPayload == null) {
            return;
        }

        this.isUploadWorkerBusy = true;
        this.UploadTimer.Restart();
        this.UploadDetailAsync(nextPayload);
    }

    private void UploadDetailAsync(QueuedDetailPayload payload) {
        Task.Run(async () => {
            try {
                var outcome = await this.TryUploadDetailAsync(payload.Detail);
                switch (outcome) {
                    case DetailUploadOutcome.Applied:
                        this.RemoveQueuedPayloadIfUnchanged(payload);
                        Interlocked.Exchange(ref this.lastSuccessfulUploadAtUtcTicks, DateTime.UtcNow.Ticks);
                        this.lastSuccessfulUploadListingId = payload.Detail.ListingId;
                        Interlocked.Increment(ref this.lastSuccessfulUploadAckVersion);
                        break;

                    case DetailUploadOutcome.ListingMissing:
                        this.RemoveQueuedPayloadIfUnchanged(payload);
                        Interlocked.Exchange(ref this.lastTerminalUploadAtUtcTicks, DateTime.UtcNow.Ticks);
                        this.lastTerminalUploadListingId = payload.Detail.ListingId;
                        Interlocked.Increment(ref this.lastTerminalUploadAckVersion);
                        Plugin.Log.Debug($"PartyDetailCollector: listing {payload.Detail.ListingId} missing on server while applying detail update; dropping queued detail payload.");
                        break;

                    default:
                        this.ScheduleRetry(payload);
                        break;
                }
            } catch (Exception e) {
                Plugin.Log.Error($"PartyDetailCollector upload error: {e.Message}");
                this.ScheduleRetry(payload);
            } finally {
                this.isUploadWorkerBusy = false;
            }
        });
    }

    private async Task<DetailUploadOutcome> TryUploadDetailAsync(UploadablePartyDetail detail) {
        var json = JsonConvert.SerializeObject(detail);
        var anyApplied = false;
        var anyMissing = false;

        foreach (var uploadUrl in this.Plugin.Configuration.UploadUrls.Where(u => u.IsEnabled)) {
            // Circuit Breaker
            if (uploadUrl.FailureCount >= this.Plugin.Configuration.CircuitBreakerFailureThreshold) {
                if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < this.Plugin.Configuration.CircuitBreakerBreakDurationMinutes) {
                    continue;
                }
            }

            var baseUrl = uploadUrl.Url.TrimEnd('/');

            if (baseUrl.EndsWith("/contribute/multiple")) {
                baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
            } else if (baseUrl.EndsWith("/contribute")) {
                baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
            }

            var detailUrl = baseUrl + "/contribute/detail";

            try {
                using var request = IngestRequestFactory.CreatePostJsonRequest(
                    this.Plugin.Configuration,
                    detailUrl,
                    "/contribute/detail",
                    json
                );
                var resp = await this.Client.SendAsync(request);
                var output = await resp.Content.ReadAsStringAsync();

                if (resp.IsSuccessStatusCode) {
                    uploadUrl.FailureCount = 0;

                    if (TryParseDetailResponse(output, out var matchedCount, out var modifiedCount)) {
                        if (matchedCount == 0) {
                            anyMissing = true;
                        } else {
                            anyApplied = true;
                        }

                        Plugin.Log.Debug($"PartyDetailCollector: {detailUrl}: {resp.StatusCode} matched={matchedCount} modified={modifiedCount}");
                    } else {
                        // Legacy server fallback ("ok" plain response)
                        anyApplied = true;
                        Plugin.Log.Debug($"PartyDetailCollector: {detailUrl}: {resp.StatusCode} {output}");
                    }
                } else {
                    uploadUrl.FailureCount++;
                    uploadUrl.LastFailureTime = DateTime.UtcNow;
                    if ((int)resp.StatusCode == 429) {
                        var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(resp);
                        if (retryAfter.HasValue) {
                            Plugin.Log.Warning($"PartyDetailCollector: rate limited by {detailUrl}, retry_after={retryAfter.Value}s");
                        }
                    }

                    Plugin.Log.Debug($"PartyDetailCollector: {detailUrl}: {resp.StatusCode} {output}");
                }
            } catch (Exception ex) {
                uploadUrl.FailureCount++;
                uploadUrl.LastFailureTime = DateTime.UtcNow;
                Plugin.Log.Error($"PartyDetailCollector upload error to {detailUrl}: {ex.Message}");
            }
        }

        if (anyApplied) {
            return DetailUploadOutcome.Applied;
        }

        if (anyMissing) {
            return DetailUploadOutcome.ListingMissing;
        }

        return DetailUploadOutcome.RetryableFailure;
    }

    private static bool TryParseDetailResponse(string responseBody, out long matchedCount, out long modifiedCount) {
        matchedCount = 0;
        modifiedCount = 0;

        var trimmed = responseBody.Trim();
        if (!trimmed.StartsWith('{')) {
            return false;
        }

        try {
            var parsed = JsonConvert.DeserializeObject<ContributeDetailResponse>(trimmed);
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

    private void RemoveQueuedPayloadIfUnchanged(QueuedDetailPayload payload) {
        if (!this.PendingDetails.TryGetValue(payload.Detail.ListingId, out var existing)) {
            return;
        }

        if (existing.Fingerprint != payload.Fingerprint) {
            return;
        }

        this.PendingDetails.TryRemove(payload.Detail.ListingId, out _);
    }

    private void ScheduleRetry(QueuedDetailPayload payload) {
        if (!this.PendingDetails.TryGetValue(payload.Detail.ListingId, out var existing)) {
            return;
        }

        if (existing.Fingerprint != payload.Fingerprint) {
            return;
        }

        if (existing.AttemptCount >= MaxQueueRetryCount) {
            this.PendingDetails.TryRemove(payload.Detail.ListingId, out _);
            Plugin.Log.Warning($"PartyDetailCollector: dropping detail payload listing={payload.Detail.ListingId} after {existing.AttemptCount} retries");
            return;
        }

        var updatedAttemptCount = existing.AttemptCount + 1;
        var backoffMs = ComputeRetryBackoffMs(updatedAttemptCount);
        var nextAttemptUtc = DateTime.UtcNow.AddMilliseconds(backoffMs);

        var updatedPayload = existing with {
            AttemptCount = updatedAttemptCount,
            NextAttemptAtUtc = nextAttemptUtc,
        };

        this.PendingDetails.TryUpdate(payload.Detail.ListingId, updatedPayload, existing);
    }

    private static int ComputeRetryBackoffMs(int attemptCount) {
        var exponent = Math.Min(attemptCount, 5);
        var baseDelay = 1000 * (1 << exponent);
        var cappedDelay = Math.Min(baseDelay, 15000);
        var jitter = Random.Shared.Next(50, 250);
        return cappedDelay + jitter;
    }

    private static ulong ComputeMemberFingerprint(List<ulong> memberContentIds, List<byte> memberJobs) {
        unchecked {
            var hash = 1469598103934665603UL;
            hash = Fnv1aMix(hash, (ulong)memberContentIds.Count);

            for (var i = 0; i < memberContentIds.Count; i++) {
                hash = Fnv1aMix(hash, memberContentIds[i]);
                hash = Fnv1aMix(hash, memberJobs[i]);
            }

            return hash;
        }
    }

    private static ulong Fnv1aMix(ulong hash, ulong value) {
        unchecked {
            hash ^= value;
            hash *= 1099511628211UL;
            return hash;
        }
    }

    private sealed record QueuedDetailPayload(
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

    private enum DetailUploadOutcome {
        Applied,
        ListingMissing,
        RetryableFailure,
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal class UploadablePartyDetail {
    public uint ListingId { get; set; }
    public ulong LeaderContentId { get; set; }
    public string LeaderName { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }
    public List<ulong> MemberContentIds { get; set; } = new();
    public List<byte> MemberJobs { get; set; } = new();
}
