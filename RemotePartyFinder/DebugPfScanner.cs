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
    private const int MaxAgentListingSlots = 50;

    private readonly Plugin _plugin;
    private readonly PartyDetailCollector _detailCollector;
    private readonly Gatherer _gatherer;
    private readonly ConcurrentQueue<ListingCandidate> _incoming = new();
    private readonly Dictionary<uint, ListingCandidate> _latestVisible = new();
    private readonly Dictionary<uint, DateTime> _attemptedAt = new();
    private readonly Queue<ListingCandidate> _pending = new();
    private readonly Stopwatch _tickGate = new();

    private ScanState _state = ScanState.Idle;
    private ListingCandidate _target;
    private bool _hasTarget;
    private bool _retryTargetAfterCooldown;
    private int _openAttemptsForTarget;
    private int _consecutiveFailures;
    private int _processedCount;
    private DateTime _nextActionAtUtc = DateTime.MinValue;
    private DateTime _nextRefreshAtUtc = DateTime.MinValue;
    private DateTime _stateDeadlineUtc = DateTime.MinValue;
    private DateTime _detailReadyAtUtc = DateTime.MinValue;
    private long _waitForAckVersion = -1;
    private long _waitForTerminalAckVersion = -1;
    private DetailSnapshot _lastReadySnapshot;
    private int _readyStableTicks;
    private string _lastAttemptReason = "none";
    private bool _lastAttemptSuccess;
    private uint _lastAttemptListingId;
    private bool _wasEnabled;

    internal DebugPfScanner(Plugin plugin, PartyDetailCollector detailCollector, Gatherer gatherer) {
        _plugin = plugin;
        _detailCollector = detailCollector;
        _gatherer = gatherer;

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
    internal string LastAttemptReason => _lastAttemptReason;
    internal bool LastAttemptSuccess => _lastAttemptSuccess;
    internal uint LastAttemptListingId => _lastAttemptListingId;

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
        _nextRefreshAtUtc = DateTime.MinValue;
        _stateDeadlineUtc = DateTime.MinValue;
        _detailReadyAtUtc = DateTime.MinValue;
        _waitForAckVersion = -1;
        _waitForTerminalAckVersion = -1;
        _lastReadySnapshot = default;
        _readyStableTicks = 0;
        _lastAttemptReason = "none";
        _lastAttemptSuccess = false;
        _lastAttemptListingId = 0;
    }

    private void OnListing(IPartyFinderListing listing, IPartyFinderListingEventArgs args) {
        if (!Enabled || listing.Id == 0) {
            return;
        }

        _incoming.Enqueue(new ListingCandidate(
            listing.Id,
            listing.ContentId,
            DateTime.UtcNow,
            args.BatchNumber
        ));
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
        DrainIncoming();
        PruneCaches();

        if (!_plugin.Configuration.AutoDetailScanCurrentPageOnly) {
            MergeAgentListingIds();
        }

        if (_plugin.Configuration.AutoDetailScanMaxPerRun > 0
            && _processedCount >= _plugin.Configuration.AutoDetailScanMaxPerRun) {
            SetIdle();
            return;
        }

        if (_pending.Count == 0) {
            RebuildPendingQueue();
        }

        if (!TryTakeNextReadyTarget(out var nextTarget)) {
            TryRequestListingsUpdate();
            _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
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

            if (!IsListingReadyForDetail(candidate)) {
                _pending.Enqueue(candidate);
                continue;
            }

            target = candidate;
            return true;
        }

        target = default;
        return false;
    }

    private bool IsListingReadyForDetail(ListingCandidate candidate) {
        return _gatherer.WasListingUploadedAfter(candidate.ListingId, candidate.SeenAtUtc);
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
            var timeoutMs = Math.Clamp(_plugin.Configuration.AutoDetailScanDetailTimeoutMs, 500, 10000);
            _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
            _waitForAckVersion = -1;
            _waitForTerminalAckVersion = -1;
            _state = ScanState.WaitDetailReady;
            return;
        }

        if (_openAttemptsForTarget < 2) {
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
                _detailReadyAtUtc = DateTime.UtcNow;
                _waitForAckVersion = _detailCollector.LastSuccessfulUploadAckVersion;
                _waitForTerminalAckVersion = _detailCollector.LastTerminalUploadAckVersion;
                var timeoutMs = Math.Clamp(_plugin.Configuration.AutoDetailScanDetailTimeoutMs, 500, 10000);
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

        if (_openAttemptsForTarget < 2) {
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

        var minDwellMs = Math.Clamp(_plugin.Configuration.AutoDetailScanMinDwellMs, 100, 5000);
        var dwellElapsedMs = (DateTime.UtcNow - _detailReadyAtUtc).TotalMilliseconds;
        var ackVersion = _detailCollector.LastSuccessfulUploadAckVersion;
        var hasAppliedAck = ackVersion > _waitForAckVersion
                            && _detailCollector.LastSuccessfulUploadListingId == _target.ListingId;
        var terminalAckVersion = _detailCollector.LastTerminalUploadAckVersion;
        var hasTerminalAck = terminalAckVersion > _waitForTerminalAckVersion
                             && _detailCollector.LastTerminalUploadListingId == _target.ListingId;

        if (dwellElapsedMs >= minDwellMs && hasAppliedAck) {
            MarkAttempt(true, "collected");
            return;
        }

        if (dwellElapsedMs >= minDwellMs && hasTerminalAck) {
            MarkAttempt(true, "listing_missing");
            return;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        if (hasAppliedAck) {
            MarkAttempt(true, "collector_delayed");
            return;
        }

        if (hasTerminalAck) {
            MarkAttempt(true, "listing_missing_delayed");
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
    }

    private void DrainIncoming() {
        while (_incoming.TryDequeue(out var listing)) {
            UpsertVisibleCandidate(listing);
        }
    }

    private void PruneCaches() {
        var now = DateTime.UtcNow;

        // Keep visible cache fresh enough to represent the current PF page.
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

    private void RebuildPendingQueue() {
        _pending.Clear();
        foreach (var listing in _latestVisible.Values
                     .OrderBy(value => value.SeenAtUtc)
                     .ThenByDescending(value => value.BatchNumber)) {
            if (IsRecentlyAttempted(listing.ListingId)) {
                continue;
            }

            _pending.Enqueue(listing);
        }
    }

    private unsafe void TryRequestListingsUpdate(bool force = false) {
        var now = DateTime.UtcNow;
        if (!force && now < _nextRefreshAtUtc) {
            return;
        }

        var refreshMs = Math.Clamp(_plugin.Configuration.AutoDetailScanRefreshIntervalMs, 1000, 60000);
        _nextRefreshAtUtc = now.AddMilliseconds(refreshMs);

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return;
        }

        var ok = agent->RequestListingsUpdate();
        Plugin.Log.Debug($"DebugPfScanner: RequestListingsUpdate -> {ok}");
    }

    private unsafe void MergeAgentListingIds() {
        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return;
        }

        var now = DateTime.UtcNow;
        for (var i = 0; i < MaxAgentListingSlots; i++) {
            var rawListingId = agent->Listings.ListingIds[i];
            if (rawListingId == 0 || rawListingId > uint.MaxValue) {
                continue;
            }

            var listingId = (uint)rawListingId;
            UpsertVisibleCandidate(new ListingCandidate(
                listingId,
                0,
                now,
                int.MaxValue - i
            ));
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

    private void StartRunFromFirstPage() {
        ResetSession();
        TryRequestListingsUpdate(force: true);

        var bootstrapWaitMs = Math.Clamp(_plugin.Configuration.AutoDetailScanActionIntervalMs, 100, 2000) + 200;
        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(bootstrapWaitMs);
        _state = ScanState.Cooldown;

        Plugin.Log.Debug($"DebugPfScanner: starting run from first page refresh (current_page_only={_plugin.Configuration.AutoDetailScanCurrentPageOnly})");
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
