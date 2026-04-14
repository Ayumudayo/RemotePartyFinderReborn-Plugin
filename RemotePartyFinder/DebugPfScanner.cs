using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Dalamud.Game.Gui.PartyFinder.Types;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;

namespace RemotePartyFinder;

internal sealed class DebugPfScanner : IDisposable {
    private const int MinCollectionTimeoutMs = 1500;
    private const int CollectionTimeoutExtraBudgetMs = 700;

    private readonly Plugin _plugin;
    private readonly PartyDetailCollector _detailCollector;

    private readonly ConcurrentQueue<ListingCandidate> _incoming = new();
    private readonly Dictionary<uint, ListingCandidate> _latestVisible = new();
    private readonly Dictionary<uint, ListingCandidate> _collectedListings = new();
    private readonly Dictionary<uint, DateTime> _attemptedAt = new();
    private readonly Queue<ListingCandidate> _pending = new();
    private readonly List<ListingCandidate> _collectedRunSnapshot = new();
    private readonly object _collectionLock = new();
    private readonly Stopwatch _tickGate = new();

    private ScanState _state = ScanState.Idle;
    private ListingCandidate _target;
    private bool _hasTarget;
    private bool _retryTargetAfterCooldown;
    private int _openAttemptsForTarget;
    private int _consecutiveFailures;
    private int _processedCount;
    private DateTime _nextActionAtUtc = DateTime.MinValue;
    private DateTime _stateDeadlineUtc = DateTime.MinValue;
    private long _waitForQueueAckVersion = -1;
    private long _waitForAckVersion = -1;
    private long _waitForTerminalAckVersion = -1;
    private DetailSnapshot _lastReadySnapshot;
    private int _readyStableTicks;
    private string _lastAttemptReason = "none";
    private bool _lastAttemptSuccess;
    private uint _lastAttemptListingId;
    private bool _wasEnabled;
    private bool _runFromCollectedListings;

    internal DebugPfScanner(Plugin plugin, PartyDetailCollector detailCollector, Gatherer _) {
        _plugin = plugin;
        _detailCollector = detailCollector;

        _plugin.PartyFinderGui.ReceiveListing += OnListing;
        _plugin.Framework.Update += OnUpdate;
        _tickGate.Start();
    }

    internal bool Enabled => _plugin.Configuration.EnableAutoDetailScanDebug;
    internal string StateName => _state.ToString();
    internal uint CurrentTargetListingId => _hasTarget ? _target.ListingId : 0;
    internal int PendingCount => _pending.Count;
    internal int ProcessedCount => _processedCount;
    internal int ConsecutiveFailures => _consecutiveFailures;
    internal int VisibleListingCount => _latestVisible.Count;
    internal int CollectedListingCount {
        get {
            lock (_collectionLock) {
                return _collectedListings.Count;
            }
        }
    }
    internal string LastAttemptReason => _lastAttemptReason;
    internal bool LastAttemptSuccess => _lastAttemptSuccess;
    internal uint LastAttemptListingId => _lastAttemptListingId;

    internal int ClearCollectedListings() {
        lock (_collectionLock) {
            var removed = _collectedListings.Count;
            _collectedListings.Clear();
            _collectedRunSnapshot.Clear();
            return removed;
        }
    }

    internal int CollectCurrentPageSnapshot() {
        unsafe {
            var agent = AgentLookingForGroup.Instance();
            if (agent == null) {
                return 0;
            }

            var added = 0;
            var now = DateTime.UtcNow;
            for (var index = 0; index < 50; index++) {
                var rawListingId = agent->Listings.ListingIds[index];
                if (rawListingId == 0 || rawListingId > uint.MaxValue) {
                    continue;
                }

                var listingId = (uint)rawListingId;
                var candidate = new ListingCandidate(listingId, 0, now.AddMilliseconds(index), 1000 - index);
                if (UpsertCollectedCandidate(candidate)) {
                    added++;
                }
            }

            return added;
        }
    }

    internal bool StartCollectedBatchRun(out string status) {
        var snapshotCount = 0;
        lock (_collectionLock) {
            if (_collectedListings.Count == 0) {
                status = "no collected listings; page through PF first or capture current page snapshot.";
                return false;
            }

            _collectedRunSnapshot.Clear();
            _collectedRunSnapshot.AddRange(_collectedListings.Values
                .OrderBy(value => value.SeenAtUtc)
                .ThenByDescending(value => value.BatchNumber));
            snapshotCount = _collectedRunSnapshot.Count;
        }

        _runFromCollectedListings = true;
        ResetSession();
        _wasEnabled = false;

        if (!_plugin.Configuration.EnableAutoDetailScanDebug) {
            _plugin.Configuration.EnableAutoDetailScanDebug = true;
            _plugin.Configuration.Save();
        }

        status = $"scheduled batch detail-click run for {snapshotCount} collected listings.";
        return true;
    }

    public void Dispose() {
        _plugin.Framework.Update -= OnUpdate;
        _plugin.PartyFinderGui.ReceiveListing -= OnListing;
    }

    internal void ResetSession() {
        _incoming.Clear();
        _latestVisible.Clear();
        _attemptedAt.Clear();
        _pending.Clear();

        _state = ScanState.Idle;
        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
        _consecutiveFailures = 0;
        _processedCount = 0;
        _nextActionAtUtc = DateTime.MinValue;
        _stateDeadlineUtc = DateTime.MinValue;
        _waitForQueueAckVersion = -1;
        _waitForAckVersion = -1;
        _waitForTerminalAckVersion = -1;
        _lastReadySnapshot = default;
        _readyStableTicks = 0;
        _lastAttemptReason = "none";
        _lastAttemptSuccess = false;
        _lastAttemptListingId = 0;
    }

    private void OnListing(IPartyFinderListing listing, IPartyFinderListingEventArgs args) {
        if (listing.Id == 0) {
            return;
        }

        var candidate = new ListingCandidate(
            listing.Id,
            listing.ContentId,
            DateTime.UtcNow,
            args.BatchNumber
        );

        if (_plugin.Configuration.EnableManualPageCollectionDebug) {
            UpsertCollectedCandidate(candidate);
        }

        if (!Enabled || _runFromCollectedListings) {
            return;
        }

        _incoming.Enqueue(candidate);
    }

    private void OnUpdate(IFramework framework) {
        if (_tickGate.ElapsedMilliseconds < 50) {
            return;
        }

        _tickGate.Restart();

        if (!Enabled) {
            if (_state != ScanState.Idle || _pending.Count > 0 || _processedCount > 0 || _consecutiveFailures > 0) {
                ResetSession();
            }

            _runFromCollectedListings = false;
            lock (_collectionLock) {
                _collectedRunSnapshot.Clear();
            }
            _wasEnabled = false;
            return;
        }

        if (!_wasEnabled) {
            _wasEnabled = true;
            StartRunFromFirstPage();
            return;
        }

        if (!IsLookingForGroupOpen()) {
            SetIdle();
            return;
        }

        var maxFailures = Math.Max(_plugin.Configuration.AutoDetailScanMaxConsecutiveFailures, 1);
        if (_consecutiveFailures >= maxFailures) {
            return;
        }

        switch (_state) {
            case ScanState.Idle:
                _state = ScanState.SyncQueue;
                break;

            case ScanState.SyncQueue:
                SyncQueue();
                break;

            case ScanState.OpenTarget:
                OpenTarget();
                break;

            case ScanState.WaitDetailReady:
                WaitDetailReady();
                break;

            case ScanState.WaitCollected:
                WaitCollected();
                break;

            case ScanState.Cooldown:
                if (DateTime.UtcNow >= _nextActionAtUtc) {
                    if (_retryTargetAfterCooldown && _hasTarget) {
                        _retryTargetAfterCooldown = false;
                        _state = ScanState.OpenTarget;
                    } else {
                        _state = ScanState.SyncQueue;
                    }
                }
                break;
        }
    }

    private void SyncQueue() {
        if (!_runFromCollectedListings) {
            DrainIncoming();
        }
        PruneCaches();

        if (_plugin.Configuration.AutoDetailScanMaxPerRun > 0
            && _processedCount >= _plugin.Configuration.AutoDetailScanMaxPerRun) {
            CompleteRun("max_per_run");
            return;
        }

        if (_pending.Count == 0) {
            RebuildPendingQueue();
        }

        if (!TryTakeNextReadyTarget(out var nextTarget)) {
            var now = DateTime.UtcNow;

            if (_pending.Count > 0 || (!_runFromCollectedListings && !_incoming.IsEmpty)) {
                _nextActionAtUtc = now.AddMilliseconds(250);
                _state = ScanState.Cooldown;
                return;
            }

            if (_detailCollector.PendingQueueCount == 0) {
                var completionReason = _runFromCollectedListings ? "collected_batch_complete" : "current_page_complete";
                CompleteRun(completionReason);
                return;
            }

            _nextActionAtUtc = now.AddMilliseconds(250);
            _state = ScanState.Cooldown;
            return;
        }

        _target = nextTarget;
        _hasTarget = true;
        _openAttemptsForTarget = 0;
        _retryTargetAfterCooldown = false;
        _state = ScanState.OpenTarget;
    }

    private bool TryTakeNextReadyTarget(out ListingCandidate target) {
        var remaining = _pending.Count;
        while (remaining-- > 0) {
            var candidate = _pending.Dequeue();

            if (IsRecentlyAttempted(candidate.ListingId)) {
                continue;
            }

            target = candidate;
            return true;
        }

        target = default;
        return false;
    }

    private void OpenTarget() {
        if (!_hasTarget) {
            _state = ScanState.SyncQueue;
            return;
        }

        if (DateTime.UtcNow < _nextActionAtUtc) {
            return;
        }

        var opened = TryOpenTarget(_target);
        _openAttemptsForTarget++;

        var actionIntervalMs = Math.Clamp(_plugin.Configuration.AutoDetailScanActionIntervalMs, 100, 2000);
        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(actionIntervalMs);

        if (opened) {
            var timeoutMs = GetDetailReadyTimeoutMs();
            _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
            _waitForQueueAckVersion = _detailCollector.LastQueuedAckVersion;
            _waitForAckVersion = _detailCollector.LastSuccessfulUploadAckVersion;
            _waitForTerminalAckVersion = _detailCollector.LastTerminalUploadAckVersion;
            _state = ScanState.WaitDetailReady;
            return;
        }

        if (ShouldRetryTargetAfterFailure(_openAttemptsForTarget, _plugin.Configuration.AutoDetailScanRetryCount)) {
            _retryTargetAfterCooldown = true;
            _state = ScanState.Cooldown;
            return;
        }

        MarkAttempt(false, "open_failed");
    }

    private unsafe bool TryOpenTarget(ListingCandidate target) {
        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return false;
        }

        if (agent->OpenListing(target.ListingId)) {
            return true;
        }

        return target.ContentId != 0 && agent->OpenListingByContentId(target.ContentId);
    }

    private void WaitDetailReady() {
        if (!_hasTarget) {
            _state = ScanState.SyncQueue;
            return;
        }

        var snapshot = GetCurrentDetailSnapshot();
        var listingMatches = snapshot.ListingId == _target.ListingId;
        var leaderMatches = _target.ContentId == 0 || snapshot.LeaderContentId == _target.ContentId;
        var hasMembers = snapshot.NonZeroMembers > 0;

        if (listingMatches && leaderMatches && hasMembers) {
            if (_readyStableTicks > 0 && snapshot.Equals(_lastReadySnapshot)) {
                _readyStableTicks++;
            } else {
                _lastReadySnapshot = snapshot;
                _readyStableTicks = 1;
            }

            var requiredStableTicks = _target.ContentId == 0 ? 4 : 2;
            if (_readyStableTicks >= requiredStableTicks) {
                var minDwellMs = Math.Clamp(_plugin.Configuration.AutoDetailScanMinDwellMs, 100, 5000);
                var timeoutMs = GetDetailCollectionTimeoutMs(minDwellMs);
                _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(timeoutMs);
                _state = ScanState.WaitCollected;
                return;
            }
        } else {
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        if (ShouldRetryTargetAfterFailure(_openAttemptsForTarget, _plugin.Configuration.AutoDetailScanRetryCount)) {
            _retryTargetAfterCooldown = true;
            _state = ScanState.Cooldown;
            return;
        }

        MarkAttempt(false, "detail_timeout");
    }

    private void WaitCollected() {
        if (!_hasTarget) {
            _state = ScanState.SyncQueue;
            return;
        }

        var queueAckVersion = _detailCollector.LastQueuedAckVersion;
        var hasQueuedAck = queueAckVersion > _waitForQueueAckVersion
                           && _detailCollector.LastUploadedListingId == _target.ListingId;

        if (hasQueuedAck) {
            MarkAttempt(true, "queued");
            return;
        }

        var ackVersion = _detailCollector.LastSuccessfulUploadAckVersion;
        var hasAppliedAck = ackVersion > _waitForAckVersion
                            && _detailCollector.LastSuccessfulUploadListingId == _target.ListingId;
        var terminalAckVersion = _detailCollector.LastTerminalUploadAckVersion;
        var hasTerminalAck = terminalAckVersion > _waitForTerminalAckVersion
                             && _detailCollector.LastTerminalUploadListingId == _target.ListingId;

        if (hasAppliedAck) {
            MarkAttempt(true, "collected");
            return;
        }

        if (hasTerminalAck) {
            MarkAttempt(true, "listing_missing");
            return;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        MarkAttempt(false, "collector_timeout");
    }

    private void MarkAttempt(bool success, string reason) {
        if (_hasTarget) {
            _attemptedAt[_target.ListingId] = DateTime.UtcNow;
            Plugin.Log.Debug($"DebugPfScanner: listing={_target.ListingId} success={success} reason={reason}");
            _lastAttemptListingId = _target.ListingId;
        }

        _lastAttemptReason = reason;
        _lastAttemptSuccess = success;

        if (success) {
            _processedCount++;
            _consecutiveFailures = 0;
        } else {
            _consecutiveFailures++;
        }

        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
        _waitForQueueAckVersion = -1;
        _waitForAckVersion = -1;
        _waitForTerminalAckVersion = -1;
        _readyStableTicks = 0;
        _lastReadySnapshot = default;

        var cooldownMs = Math.Clamp(_plugin.Configuration.AutoDetailScanPostListingCooldownMs, 50, 3000);
        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(cooldownMs);
        _state = ScanState.Cooldown;
    }

    private void SetIdle() {
        _state = ScanState.Idle;
        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
        _runFromCollectedListings = false;
        lock (_collectionLock) {
            _collectedRunSnapshot.Clear();
        }
    }

    private void CompleteRun(string reason) {
        Plugin.Log.Debug($"DebugPfScanner: complete reason={reason} processed={_processedCount} failures={_consecutiveFailures}");
        SetIdle();

        if (!_plugin.Configuration.EnableAutoDetailScanDebug) {
            return;
        }

        _plugin.Configuration.EnableAutoDetailScanDebug = false;
        _plugin.Configuration.Save();
    }

    private void DrainIncoming() {
        while (_incoming.TryDequeue(out var listing)) {
            UpsertVisibleCandidate(listing);
        }
    }

    private void PruneCaches() {
        var now = DateTime.UtcNow;

        var visibleTtl = TimeSpan.FromSeconds(30);
        foreach (var staleId in _latestVisible
                     .Where(kvp => now - kvp.Value.SeenAtUtc > visibleTtl)
                     .Select(kvp => kvp.Key)
                     .ToList()) {
            _latestVisible.Remove(staleId);
        }

        var dedupTtl = TimeSpan.FromSeconds(Math.Clamp(_plugin.Configuration.AutoDetailScanDedupTtlSeconds, 30, 3600));
        foreach (var staleId in _attemptedAt
                     .Where(kvp => now - kvp.Value > dedupTtl)
                     .Select(kvp => kvp.Key)
                     .ToList()) {
            _attemptedAt.Remove(staleId);
        }
    }

    private bool IsRecentlyAttempted(uint listingId) {
        if (!_attemptedAt.TryGetValue(listingId, out var lastAttemptUtc)) {
            return false;
        }

        var dedupTtl = TimeSpan.FromSeconds(Math.Clamp(_plugin.Configuration.AutoDetailScanDedupTtlSeconds, 30, 3600));
        return DateTime.UtcNow - lastAttemptUtc < dedupTtl;
    }

    internal static bool ShouldRetryTargetAfterFailure(int attemptsMade, int configuredRetries) {
        return attemptsMade <= PartyDetailCollector.NormalizeRetryCount(configuredRetries);
    }

    private void RebuildPendingQueue() {
        _pending.Clear();

        if (_runFromCollectedListings) {
            List<ListingCandidate> snapshot;
            lock (_collectionLock) {
                snapshot = _collectedRunSnapshot.ToList();
            }

            foreach (var listing in snapshot) {
                if (IsRecentlyAttempted(listing.ListingId)) {
                    continue;
                }

                _pending.Enqueue(listing);
            }

            return;
        }

        foreach (var listing in _latestVisible.Values
                     .OrderBy(value => value.SeenAtUtc)
                     .ThenByDescending(value => value.BatchNumber)) {
            if (IsRecentlyAttempted(listing.ListingId)) {
                continue;
            }

            _pending.Enqueue(listing);
        }
    }

    private void UpsertVisibleCandidate(ListingCandidate incoming) {
        if (_latestVisible.TryGetValue(incoming.ListingId, out var existing)) {
            _latestVisible[incoming.ListingId] = new ListingCandidate(
                incoming.ListingId,
                incoming.ContentId != 0 ? incoming.ContentId : existing.ContentId,
                existing.SeenAtUtc,
                Math.Max(existing.BatchNumber, incoming.BatchNumber)
            );
            return;
        }

        _latestVisible[incoming.ListingId] = incoming;
    }

    private bool UpsertCollectedCandidate(ListingCandidate incoming) {
        lock (_collectionLock) {
            if (_collectedListings.TryGetValue(incoming.ListingId, out var existing)) {
                _collectedListings[incoming.ListingId] = new ListingCandidate(
                    incoming.ListingId,
                    incoming.ContentId != 0 ? incoming.ContentId : existing.ContentId,
                    existing.SeenAtUtc,
                    Math.Max(existing.BatchNumber, incoming.BatchNumber)
                );
                return false;
            }

            _collectedListings[incoming.ListingId] = incoming;
            return true;
        }
    }

    private int SeedPendingFromCollectedListings() {
        List<ListingCandidate> snapshot;
        lock (_collectionLock) {
            snapshot = _collectedRunSnapshot.ToList();
        }

        var seeded = 0;
        foreach (var listing in snapshot) {
            _pending.Enqueue(listing);
            _latestVisible[listing.ListingId] = listing;
            seeded++;
        }

        return seeded;
    }

    private int GetDetailReadyTimeoutMs() {
        return Math.Clamp(_plugin.Configuration.AutoDetailScanDetailTimeoutMs, 500, 10000);
    }

    private int GetDetailCollectionTimeoutMs(int minDwellMs) {
        var configuredTimeoutMs = GetDetailReadyTimeoutMs();
        var timeoutFloorFromDwellMs = minDwellMs + CollectionTimeoutExtraBudgetMs;
        return Math.Clamp(Math.Max(configuredTimeoutMs, timeoutFloorFromDwellMs), MinCollectionTimeoutMs, 10000);
    }

    private void StartRunFromFirstPage() {
        ResetSession();
        if (_runFromCollectedListings) {
            var seeded = SeedPendingFromCollectedListings();
            if (seeded == 0) {
                CompleteRun("collected_batch_empty");
                return;
            }

            _state = ScanState.SyncQueue;
            _nextActionAtUtc = DateTime.UtcNow;
            Plugin.Log.Debug($"DebugPfScanner: starting deferred batch run from collected listings (seeded_listings={seeded}).");
            return;
        }

        var currentPageSeeded = SeedVisibleCandidatesFromCurrentPage();
        _state = ScanState.SyncQueue;
        _nextActionAtUtc = DateTime.UtcNow;
        Plugin.Log.Debug($"DebugPfScanner: starting run from current PF page only (seeded_listings={currentPageSeeded}).");
    }

    private int SeedVisibleCandidatesFromCurrentPage() {
        unsafe {
            var agent = AgentLookingForGroup.Instance();
            if (agent == null) {
                return 0;
            }

            var seeded = 0;
            var now = DateTime.UtcNow;
            for (var index = 0; index < 50; index++) {
                var rawListingId = agent->Listings.ListingIds[index];
                if (rawListingId == 0 || rawListingId > uint.MaxValue) {
                    continue;
                }

                var listingId = (uint)rawListingId;
                var seenAtUtc = now.AddMilliseconds(index);
                var batchNumber = 1000 - index;
                UpsertVisibleCandidate(new ListingCandidate(listingId, 0, seenAtUtc, batchNumber));
                seeded++;
            }

            return seeded;
        }
    }

    private bool IsLookingForGroupOpen() {
        return _plugin.GameGui.GetAddonByName("LookingForGroup", 1) != 0;
    }

    private unsafe DetailSnapshot GetCurrentDetailSnapshot() {
        if (_plugin.GameGui.GetAddonByName("LookingForGroupDetail", 1) == 0) {
            return default;
        }

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return default;
        }

        ref var detailed = ref agent->LastViewedListing;
        var effectiveParties = Math.Max(1, (int)detailed.NumberOfParties);
        var declaredSlots = Math.Max((int)detailed.TotalSlots, effectiveParties * 8);
        var totalSlots = Math.Clamp(declaredSlots, 0, 48);
        var nonZeroMembers = 0;

        for (var i = 0; i < totalSlots; i++) {
            if (detailed.MemberContentIds[i] != 0) {
                nonZeroMembers++;
            }
        }

        return new DetailSnapshot(
            detailed.ListingId,
            detailed.LeaderContentId,
            nonZeroMembers,
            totalSlots
        );
    }

    private readonly record struct DetailSnapshot(uint ListingId, ulong LeaderContentId, int NonZeroMembers, int TotalSlots);
    private readonly record struct ListingCandidate(uint ListingId, ulong ContentId, DateTime SeenAtUtc, int BatchNumber);

    private enum ScanState {
        Idle,
        SyncQueue,
        OpenTarget,
        WaitDetailReady,
        WaitCollected,
        Cooldown,
    }
}
