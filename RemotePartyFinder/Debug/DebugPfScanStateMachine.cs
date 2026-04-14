using System;
using System.Collections.Generic;

namespace RemotePartyFinder;

internal readonly record struct DebugPfListingCandidate(uint ListingId, ulong ContentId, DateTime SeenAtUtc, int BatchNumber);

internal readonly record struct DebugPfDetailSnapshot(uint ListingId, ulong LeaderContentId, int NonZeroMembers, int TotalSlots);

internal readonly record struct DebugPfCollectorAckSnapshot(
    long QueueAckVersion,
    uint QueuedListingId,
    long SuccessfulAckVersion,
    uint SuccessfulListingId,
    long TerminalAckVersion,
    uint TerminalListingId
);

internal enum DebugPfScanState {
    Idle,
    SyncQueue,
    OpenTarget,
    WaitDetailReady,
    WaitCollected,
    Cooldown,
}

internal sealed class DebugPfScanStateMachine {
    private const int MinCollectionTimeoutMs = 1500;
    private const int CollectionTimeoutExtraBudgetMs = 700;

    private readonly DebugPfListingQueue _queue;

    private DebugPfScanState _state = DebugPfScanState.Idle;
    private DebugPfListingCandidate _target;
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
    private DebugPfDetailSnapshot _lastReadySnapshot;
    private int _readyStableTicks;
    private string _lastAttemptReason = "none";
    private bool _lastAttemptSuccess;
    private uint _lastAttemptListingId;

    internal DebugPfScanStateMachine(DebugPfListingQueue queue) {
        _queue = queue;
    }

    internal DebugPfScanState State => _state;
    internal string StateName => _state.ToString();
    internal DebugPfListingCandidate CurrentTarget => _target;
    internal uint CurrentTargetListingId => _hasTarget ? _target.ListingId : 0;
    internal int PendingCount => _queue.PendingCount;
    internal int ProcessedCount => _processedCount;
    internal int ConsecutiveFailures => _consecutiveFailures;
    internal int VisibleListingCount => _queue.VisibleCount;
    internal string LastAttemptReason => _lastAttemptReason;
    internal bool LastAttemptSuccess => _lastAttemptSuccess;
    internal uint LastAttemptListingId => _lastAttemptListingId;

    internal void Reset() {
        _queue.Reset();

        _state = DebugPfScanState.Idle;
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

    internal void UpsertVisibleCandidate(DebugPfListingCandidate candidate) {
        _queue.UpsertVisible(candidate);
    }

    internal void StartSyncQueue() {
        _state = DebugPfScanState.SyncQueue;
    }

    internal bool IsActionReady(DateTime nowUtc) {
        return nowUtc >= _nextActionAtUtc;
    }

    internal string SyncQueue(
        DateTime nowUtc,
        IReadOnlyCollection<DebugPfListingCandidate> collectedSnapshot,
        bool hasIncomingListings,
        int maxPerRun,
        int dedupTtlSeconds,
        bool runFromCollectedListings
    ) {
        _queue.PruneCaches(nowUtc, dedupTtlSeconds);

        if (maxPerRun > 0 && _processedCount >= maxPerRun) {
            return "max_per_run";
        }

        if (_queue.PendingCount == 0) {
            if (runFromCollectedListings) {
                _queue.RebuildPendingQueueFromSnapshot(collectedSnapshot, nowUtc, dedupTtlSeconds);
            } else {
                _queue.RebuildPendingQueueFromVisible(nowUtc, dedupTtlSeconds);
            }
        }

        if (!_queue.TryTakeNextReadyTarget(nowUtc, dedupTtlSeconds, out var nextTarget)) {
            if (!ShouldCompleteRunWhenNoReadyTarget(_queue.PendingCount, hasIncomingListings)) {
                _nextActionAtUtc = nowUtc.AddMilliseconds(250);
                _state = DebugPfScanState.Cooldown;
                return null;
            }

            return runFromCollectedListings ? "collected_batch_complete" : "current_page_complete";
        }

        _target = nextTarget;
        _hasTarget = true;
        _openAttemptsForTarget = 0;
        _retryTargetAfterCooldown = false;
        _state = DebugPfScanState.OpenTarget;
        return null;
    }

    internal void HandleOpenAttemptResult(
        DateTime nowUtc,
        bool opened,
        int actionIntervalMs,
        int detailReadyTimeoutMs,
        int configuredRetries,
        int postListingCooldownMs,
        DebugPfCollectorAckSnapshot ackSnapshot
    ) {
        if (!_hasTarget) {
            _state = DebugPfScanState.SyncQueue;
            return;
        }

        _openAttemptsForTarget++;
        _nextActionAtUtc = nowUtc.AddMilliseconds(Math.Clamp(actionIntervalMs, 100, 2000));

        if (opened) {
            _stateDeadlineUtc = nowUtc.AddMilliseconds(GetDetailReadyTimeoutMs(detailReadyTimeoutMs));
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
            _waitForQueueAckVersion = ackSnapshot.QueueAckVersion;
            _waitForAckVersion = ackSnapshot.SuccessfulAckVersion;
            _waitForTerminalAckVersion = ackSnapshot.TerminalAckVersion;
            _state = DebugPfScanState.WaitDetailReady;
            return;
        }

        if (ShouldRetryTargetAfterFailure(_openAttemptsForTarget, configuredRetries)) {
            _retryTargetAfterCooldown = true;
            _state = DebugPfScanState.Cooldown;
            return;
        }

        MarkAttempt(nowUtc, success: false, "open_failed", postListingCooldownMs);
    }

    internal void HandleDetailReadyState(
        DateTime nowUtc,
        DebugPfDetailSnapshot snapshot,
        int minDwellMs,
        int detailReadyTimeoutMs,
        int configuredRetries,
        int postListingCooldownMs
    ) {
        if (!_hasTarget) {
            _state = DebugPfScanState.SyncQueue;
            return;
        }

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
                _stateDeadlineUtc = nowUtc.AddMilliseconds(GetDetailCollectionTimeoutMs(detailReadyTimeoutMs, minDwellMs));
                _state = DebugPfScanState.WaitCollected;
                return;
            }
        } else {
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
        }

        if (nowUtc <= _stateDeadlineUtc) {
            return;
        }

        if (ShouldRetryTargetAfterFailure(_openAttemptsForTarget, configuredRetries)) {
            _retryTargetAfterCooldown = true;
            _state = DebugPfScanState.Cooldown;
            return;
        }

        MarkAttempt(nowUtc, success: false, "detail_timeout", postListingCooldownMs);
    }

    internal void HandleCollectedState(DateTime nowUtc, DebugPfCollectorAckSnapshot ackSnapshot, int postListingCooldownMs) {
        if (!_hasTarget) {
            _state = DebugPfScanState.SyncQueue;
            return;
        }

        var hasQueuedAck = ackSnapshot.QueueAckVersion > _waitForQueueAckVersion
                           && ackSnapshot.QueuedListingId == _target.ListingId;
        if (hasQueuedAck) {
            MarkAttempt(nowUtc, success: true, "queued", postListingCooldownMs);
            return;
        }

        var hasAppliedAck = ackSnapshot.SuccessfulAckVersion > _waitForAckVersion
                            && ackSnapshot.SuccessfulListingId == _target.ListingId;
        if (hasAppliedAck) {
            MarkAttempt(nowUtc, success: true, "collected", postListingCooldownMs);
            return;
        }

        var hasTerminalAck = ackSnapshot.TerminalAckVersion > _waitForTerminalAckVersion
                             && ackSnapshot.TerminalListingId == _target.ListingId;
        if (hasTerminalAck) {
            MarkAttempt(nowUtc, success: true, "listing_missing", postListingCooldownMs);
            return;
        }

        if (nowUtc <= _stateDeadlineUtc) {
            return;
        }

        MarkAttempt(nowUtc, success: false, "collector_timeout", postListingCooldownMs);
    }

    internal void AdvanceCooldown(DateTime nowUtc) {
        if (nowUtc < _nextActionAtUtc) {
            return;
        }

        if (_retryTargetAfterCooldown && _hasTarget) {
            _retryTargetAfterCooldown = false;
            _state = DebugPfScanState.OpenTarget;
            return;
        }

        _state = DebugPfScanState.SyncQueue;
    }

    internal void SetIdle() {
        _state = DebugPfScanState.Idle;
        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
    }

    internal static bool ShouldRetryTargetAfterFailure(int attemptsMade, int configuredRetries) {
        return attemptsMade <= PartyDetailCollector.NormalizeRetryCount(configuredRetries);
    }

    internal static bool ShouldCompleteRunWhenNoReadyTarget(int pendingTargets, bool hasIncomingListings) {
        return pendingTargets <= 0 && !hasIncomingListings;
    }

    private void MarkAttempt(DateTime nowUtc, bool success, string reason, int postListingCooldownMs) {
        if (_hasTarget) {
            _queue.RecordAttempt(_target.ListingId, nowUtc);
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
        _nextActionAtUtc = nowUtc.AddMilliseconds(Math.Clamp(postListingCooldownMs, 50, 3000));
        _state = DebugPfScanState.Cooldown;
    }

    private static int GetDetailReadyTimeoutMs(int configuredTimeoutMs) {
        return Math.Clamp(configuredTimeoutMs, 500, 10000);
    }

    private static int GetDetailCollectionTimeoutMs(int configuredTimeoutMs, int minDwellMs) {
        var timeoutFloorFromDwellMs = minDwellMs + CollectionTimeoutExtraBudgetMs;
        return Math.Clamp(Math.Max(GetDetailReadyTimeoutMs(configuredTimeoutMs), timeoutFloorFromDwellMs), MinCollectionTimeoutMs, 10000);
    }
}
