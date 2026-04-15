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
    private readonly Plugin _plugin;
    private readonly PartyDetailCaptureRuntime _detailCaptureRuntime;

    private readonly ConcurrentQueue<DebugPfListingCandidate> _incoming = new();
    private readonly Dictionary<uint, DebugPfListingCandidate> _collectedListings = new();
    private readonly List<DebugPfListingCandidate> _collectedRunSnapshot = new();
    private readonly object _collectionLock = new();
    private readonly Stopwatch _tickGate = new();
    private readonly DebugPfScanStateMachine _stateMachine = new(new DebugPfListingQueue());

    private Guid _currentCaptureAttemptId;
    private uint _currentCaptureAttemptListingId;
    private bool _wasEnabled;
    private bool _runFromCollectedListings;

    internal DebugPfScanner(
        Plugin plugin,
        PartyDetailCollector detailCollector,
        PartyDetailCaptureRuntime detailCaptureRuntime,
        Gatherer _unusedGatherer
    ) {
        _plugin = plugin;
        _detailCaptureRuntime = detailCaptureRuntime;
        _ = detailCollector;

        _plugin.PartyFinderGui.ReceiveListing += OnListing;
        _plugin.Framework.Update += OnUpdate;
        _tickGate.Start();
    }

    internal bool Enabled => _plugin.Configuration.EnableAutoDetailScanDebug;
    internal string StateName => _stateMachine.StateName;
    internal uint CurrentTargetListingId => _stateMachine.CurrentTargetListingId;
    internal int PendingCount => _stateMachine.PendingCount;
    internal int ProcessedCount => _stateMachine.ProcessedCount;
    internal int ConsecutiveFailures => _stateMachine.ConsecutiveFailures;
    internal int VisibleListingCount => _stateMachine.VisibleListingCount;
    internal int CollectedListingCount {
        get {
            lock (_collectionLock) {
                return _collectedListings.Count;
            }
        }
    }
    internal string LastAttemptReason => _stateMachine.LastAttemptReason;
    internal bool LastAttemptSuccess => _stateMachine.LastAttemptSuccess;
    internal uint LastAttemptListingId => _stateMachine.LastAttemptListingId;

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
                var candidate = new DebugPfListingCandidate(listingId, 0, now.AddMilliseconds(index), 1000 - index);
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
        _stateMachine.Reset();
        ResetCaptureAttempt();
    }

    private void OnListing(IPartyFinderListing listing, IPartyFinderListingEventArgs args) {
        if (listing.Id == 0) {
            return;
        }

        var candidate = new DebugPfListingCandidate(
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
            if (_stateMachine.State != DebugPfScanState.Idle
                || _stateMachine.PendingCount > 0
                || _stateMachine.ProcessedCount > 0
                || _stateMachine.ConsecutiveFailures > 0) {
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
        if (_stateMachine.ConsecutiveFailures >= maxFailures) {
            return;
        }

        switch (_stateMachine.State) {
            case DebugPfScanState.Idle:
                _stateMachine.StartSyncQueue();
                break;

            case DebugPfScanState.SyncQueue:
                SyncQueue();
                break;

            case DebugPfScanState.OpenTarget:
                OpenTarget();
                break;

            case DebugPfScanState.WaitDetailReady:
                WaitDetailReady();
                break;

            case DebugPfScanState.WaitCollected:
                WaitCollected();
                break;

            case DebugPfScanState.Cooldown:
                _stateMachine.AdvanceCooldown(DateTime.UtcNow);
                break;
        }
    }

    private void SyncQueue() {
        if (!_runFromCollectedListings) {
            DrainIncoming();
        }

        var nowUtc = DateTime.UtcNow;
        var hasIncomingListings = !_runFromCollectedListings && !_incoming.IsEmpty;
        var completionReason = _stateMachine.SyncQueue(
            nowUtc,
            _runFromCollectedListings ? GetCollectedRunSnapshot() : Array.Empty<DebugPfListingCandidate>(),
            hasIncomingListings,
            _plugin.Configuration.AutoDetailScanMaxPerRun,
            _plugin.Configuration.AutoDetailScanDedupTtlSeconds,
            _runFromCollectedListings
        );

        if (completionReason is not null) {
            CompleteRun(completionReason);
        }
    }

    private void OpenTarget() {
        var target = _stateMachine.CurrentTarget;
        if (target.ListingId == 0) {
            _stateMachine.StartSyncQueue();
            return;
        }

        var nowUtc = DateTime.UtcNow;
        if (!_stateMachine.IsActionReady(nowUtc)) {
            return;
        }

        var beforeAttempt = CaptureAttemptLogState();
        var opened = TryOpenTarget(target);
        var requestSerial = _currentCaptureAttemptId != Guid.Empty
            ? _detailCaptureRuntime.GetArmedScannerRequestSerial(_currentCaptureAttemptId)
            : null;
        _stateMachine.HandleOpenAttemptResult(
            nowUtc,
            opened,
            _plugin.Configuration.AutoDetailScanActionIntervalMs,
            _plugin.Configuration.AutoDetailScanDetailTimeoutMs,
            _plugin.Configuration.AutoDetailScanRetryCount,
            _plugin.Configuration.AutoDetailScanPostListingCooldownMs,
            requestSerial
        );
        LogAttemptIfChanged(beforeAttempt);
    }

    private unsafe bool TryOpenTarget(DebugPfListingCandidate target) {
        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return false;
        }

        ArmCaptureRequest(target);
        _detailCaptureRuntime.BeginScannerOpenAttempt(_currentCaptureAttemptId);
        try {
            if (agent->OpenListing(target.ListingId)) {
                return true;
            }

            return target.ContentId != 0 && agent->OpenListingByContentId(target.ContentId);
        } finally {
            _detailCaptureRuntime.EndScannerOpenAttempt(_currentCaptureAttemptId);
        }
    }

    private void WaitDetailReady() {
        var beforeAttempt = CaptureAttemptLogState();
        _stateMachine.HandleDetailReadyState(
            DateTime.UtcNow,
            GetCurrentDetailSnapshot(),
            _plugin.Configuration.AutoDetailScanMinDwellMs,
            _plugin.Configuration.AutoDetailScanDetailTimeoutMs,
            _plugin.Configuration.AutoDetailScanRetryCount,
            _plugin.Configuration.AutoDetailScanPostListingCooldownMs
        );
        LogAttemptIfChanged(beforeAttempt);
    }

    private void WaitCollected() {
        var beforeAttempt = CaptureAttemptLogState();
        var target = _stateMachine.CurrentTarget;
        var hasConsumedAck = _stateMachine.CurrentRequestSerial is { } requestSerial
                             && _plugin.PartyDetailCaptureState.IsScannerConsumedAckReady(
                                  requestSerial,
                                  target.ListingId,
                                  target.ContentId
                              );
        _stateMachine.HandleCollectedState(
            DateTime.UtcNow,
            hasConsumedAck,
            _plugin.Configuration.AutoDetailScanPostListingCooldownMs
        );
        LogAttemptIfChanged(beforeAttempt);
    }

    private void SetIdle() {
        _stateMachine.SetIdle();
        ResetCaptureAttempt();
        _runFromCollectedListings = false;
        lock (_collectionLock) {
            _collectedRunSnapshot.Clear();
        }
    }

    private void CompleteRun(string reason) {
        Plugin.Log.Debug($"DebugPfScanner: complete reason={reason} processed={_stateMachine.ProcessedCount} failures={_stateMachine.ConsecutiveFailures}");
        SetIdle();

        if (!_plugin.Configuration.EnableAutoDetailScanDebug) {
            return;
        }

        _plugin.Configuration.EnableAutoDetailScanDebug = false;
        _plugin.Configuration.Save();
    }

    private void DrainIncoming() {
        while (_incoming.TryDequeue(out var listing)) {
            _stateMachine.UpsertVisibleCandidate(listing);
        }
    }

    private bool UpsertCollectedCandidate(DebugPfListingCandidate incoming) {
        lock (_collectionLock) {
            if (_collectedListings.TryGetValue(incoming.ListingId, out var existing)) {
                _collectedListings[incoming.ListingId] = new DebugPfListingCandidate(
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

    private IReadOnlyCollection<DebugPfListingCandidate> GetCollectedRunSnapshot() {
        lock (_collectionLock) {
            return _collectedRunSnapshot.ToList();
        }
    }

    private void StartRunFromFirstPage() {
        ResetSession();
        if (_runFromCollectedListings) {
            var seeded = SeedVisibleCandidatesFromCollectedListings();
            if (seeded == 0) {
                CompleteRun("collected_batch_empty");
                return;
            }

            _stateMachine.StartSyncQueue();
            Plugin.Log.Debug($"DebugPfScanner: starting deferred batch run from collected listings (seeded_listings={seeded}).");
            return;
        }

        var currentPageSeeded = SeedVisibleCandidatesFromCurrentPage();
        _stateMachine.StartSyncQueue();
        Plugin.Log.Debug($"DebugPfScanner: starting run from current PF page only (seeded_listings={currentPageSeeded}).");
    }

    private int SeedVisibleCandidatesFromCollectedListings() {
        List<DebugPfListingCandidate> snapshot;
        lock (_collectionLock) {
            snapshot = _collectedRunSnapshot.ToList();
        }

        var seeded = 0;
        foreach (var listing in snapshot) {
            _stateMachine.UpsertVisibleCandidate(listing);
            seeded++;
        }

        return seeded;
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
                _stateMachine.UpsertVisibleCandidate(new DebugPfListingCandidate(listingId, 0, seenAtUtc, batchNumber));
                seeded++;
            }

            return seeded;
        }
    }

    private bool IsLookingForGroupOpen() {
        return _plugin.GameGui.GetAddonByName("LookingForGroup", 1) != 0;
    }

    private unsafe DebugPfDetailSnapshot GetCurrentDetailSnapshot() {
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

        return new DebugPfDetailSnapshot(
            detailed.ListingId,
            detailed.LeaderContentId,
            nonZeroMembers,
            totalSlots
        );
    }

    internal static bool ShouldRetryTargetAfterFailure(int attemptsMade, int configuredRetries) {
        return DebugPfScanStateMachine.ShouldRetryTargetAfterFailure(attemptsMade, configuredRetries);
    }

    internal static bool ShouldCompleteRunWhenNoReadyTarget(int pendingTargets, bool hasIncomingListings) {
        return DebugPfScanStateMachine.ShouldCompleteRunWhenNoReadyTarget(pendingTargets, hasIncomingListings);
    }

    private AttemptLogState CaptureAttemptLogState() {
        return new AttemptLogState(
            _stateMachine.LastAttemptListingId,
            _stateMachine.LastAttemptSuccess,
            _stateMachine.LastAttemptReason,
            _stateMachine.LastTerminalOutcome
        );
    }

    private void LogAttemptIfChanged(AttemptLogState before) {
        var after = CaptureAttemptLogState();
        if (after.Equals(before) || after.ListingId == 0) {
            return;
        }

        if (_currentCaptureAttemptId != Guid.Empty && _currentCaptureAttemptListingId == after.ListingId) {
            if (after.TerminalOutcome is { } outcome) {
                _detailCaptureRuntime.CompleteScannerRequest(_currentCaptureAttemptId, outcome);
            }

            if (_detailCaptureRuntime.GetArmedScannerRequestSerial(_currentCaptureAttemptId) is null) {
                _currentCaptureAttemptId = Guid.Empty;
                _currentCaptureAttemptListingId = 0;
            }
        }

        Plugin.Log.Debug($"DebugPfScanner: listing={after.ListingId} success={after.Success} reason={after.Reason}");
    }

    private void ArmCaptureRequest(DebugPfListingCandidate target) {
        if (_currentCaptureAttemptId == Guid.Empty || _currentCaptureAttemptListingId != target.ListingId) {
            _currentCaptureAttemptId = Guid.NewGuid();
            _currentCaptureAttemptListingId = target.ListingId;
        }

        _detailCaptureRuntime.ArmScannerRequest(_currentCaptureAttemptId, target.ListingId, target.ContentId);
    }

    private void ResetCaptureAttempt() {
        if (_currentCaptureAttemptId != Guid.Empty) {
            _detailCaptureRuntime.ClearScannerRequest(_currentCaptureAttemptId);
        } else {
            _detailCaptureRuntime.ResetScannerRequest();
        }

        _currentCaptureAttemptId = Guid.Empty;
        _currentCaptureAttemptListingId = 0;
    }

    private readonly record struct AttemptLogState(
        uint ListingId,
        bool Success,
        string Reason,
        PartyDetailScannerAttemptOutcome? TerminalOutcome
    );
}
