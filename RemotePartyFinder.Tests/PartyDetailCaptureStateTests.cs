using System;
using System.Collections.Generic;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class PartyDetailCaptureStateTests {
    [Fact]
    public void Unarmed_open_listing_starts_manual_request_cycle() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);

        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);

        Assert.Equal(PartyDetailRequestOwner.Manual, state.CurrentOwner);
        Assert.Equal(9001U, state.CurrentListingId);
    }

    [Fact]
    public void Armed_scanner_open_listing_starts_scanner_request_cycle() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var attemptId = Guid.NewGuid();

        runtime.ArmScannerRequest(attemptId, listingId: 9001U, contentId: 44UL);
        runtime.BeginScannerOpenAttempt(attemptId);
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);
        runtime.EndScannerOpenAttempt(attemptId);

        Assert.Equal(PartyDetailRequestOwner.Scanner, state.CurrentOwner);
        Assert.Equal(9001U, state.CurrentListingId);
        Assert.Equal(state.CurrentRequestSerial, runtime.GetArmedScannerRequestSerial(attemptId));
    }

    [Fact]
    public void Armed_scanner_fallback_open_reuses_same_request_cycle() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var attemptId = Guid.NewGuid();

        runtime.ArmScannerRequest(attemptId, listingId: 9001U, contentId: 44UL);
        runtime.BeginScannerOpenAttempt(attemptId);
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);
        var firstRequestSerial = state.CurrentRequestSerial;

        runtime.RaiseOpenListingByContentId(contentId: 44UL);
        runtime.EndScannerOpenAttempt(attemptId);

        Assert.Equal(firstRequestSerial, state.CurrentRequestSerial);
        Assert.Equal(firstRequestSerial, runtime.GetArmedScannerRequestSerial(attemptId));
        Assert.Equal(PartyDetailRequestOwner.Scanner, state.CurrentOwner);
    }

    [Fact]
    public void Later_manual_open_of_same_listing_is_not_misclassified_as_scanner() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var attemptId = Guid.NewGuid();

        runtime.ArmScannerRequest(attemptId, listingId: 9001U, contentId: 44UL);
        runtime.BeginScannerOpenAttempt(attemptId);
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);
        runtime.EndScannerOpenAttempt(attemptId);
        var scannerRequestSerial = state.CurrentRequestSerial;

        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);

        Assert.NotEqual(scannerRequestSerial, state.CurrentRequestSerial);
        Assert.Equal(PartyDetailRequestOwner.Manual, state.CurrentOwner);
    }

    [Fact]
    public void Armed_scanner_timeout_clears_arm() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var attemptId = Guid.NewGuid();

        runtime.ArmScannerRequest(attemptId, listingId: 9001U, contentId: 44UL);
        runtime.BeginScannerOpenAttempt(attemptId);
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);
        runtime.EndScannerOpenAttempt(attemptId);

        runtime.CompleteScannerAttempt(attemptId, PartyDetailScannerAttemptOutcome.TimedOut);

        Assert.Null(runtime.GetArmedScannerRequestSerial(attemptId));
    }

    [Fact]
    public void Armed_scanner_open_failed_keeps_attempt_request_serial() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var attemptId = Guid.NewGuid();

        runtime.ArmScannerRequest(attemptId, listingId: 9001U, contentId: 44UL);
        runtime.BeginScannerOpenAttempt(attemptId);
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);
        runtime.EndScannerOpenAttempt(attemptId);
        var requestSerial = state.CurrentRequestSerial;

        runtime.CompleteScannerAttempt(attemptId, PartyDetailScannerAttemptOutcome.OpenFailed);

        Assert.Equal(requestSerial, runtime.GetArmedScannerRequestSerial(attemptId));
    }

    [Fact]
    public void Hook_initialization_failure_degrades_to_passive_mode() {
        var state = new PartyDetailCaptureState();
        var warnings = new List<string>();
        using var runtime = new FakePartyDetailCaptureRuntime(
            state,
            new ThrowingHookFactory(),
            warnings.Add
        );

        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);

        Assert.Equal(PartyDetailRequestOwner.Manual, state.CurrentOwner);
        Assert.Single(warnings);
        Assert.Contains("failed to initialize hooks", warnings[0], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Hook_scope_creation_disposes_first_hook_when_second_creation_fails() {
        var openListingHook = new TrackingHook();

        Assert.Throws<InvalidOperationException>(() =>
            PartyDetailCaptureRuntime.CreateHookScope<TrackingHook, TrackingHook>(
                () => openListingHook,
                () => throw new InvalidOperationException("simulated create failure"),
                static hook => hook.Enable(),
                static hook => hook.Enable()
            ));

        Assert.True(openListingHook.IsDisposed);
    }

    [Fact]
    public void Hook_scope_creation_disposes_created_hooks_when_later_enable_fails() {
        var openListingHook = new TrackingHook();
        var openListingByContentIdHook = new TrackingHook();

        Assert.Throws<InvalidOperationException>(() =>
            PartyDetailCaptureRuntime.CreateHookScope<TrackingHook, TrackingHook>(
                () => openListingHook,
                () => openListingByContentIdHook,
                static hook => hook.Enable(),
                static hook => throw new InvalidOperationException("simulated enable failure")
            ));

        Assert.Equal(1, openListingHook.EnableCallCount);
        Assert.True(openListingHook.IsDisposed);
        Assert.True(openListingByContentIdHook.IsDisposed);
    }

    [Fact]
    public void State_machine_exposes_typed_open_failed_outcome() {
        var nowUtc = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc);
        var queue = new DebugPfListingQueue();
        var stateMachine = new DebugPfScanStateMachine(queue);
        var target = new DebugPfListingCandidate(9001U, 44UL, nowUtc, 1);
        stateMachine.UpsertVisibleCandidate(target);

        _ = stateMachine.SyncQueue(
            nowUtc,
            [],
            hasIncomingListings: false,
            maxPerRun: 0,
            dedupTtlSeconds: 600,
            runFromCollectedListings: false
        );

        stateMachine.HandleOpenAttemptResult(
            nowUtc,
            opened: false,
            actionIntervalMs: 400,
            detailReadyTimeoutMs: 3500,
            configuredRetries: 0,
            postListingCooldownMs: 300,
            ackSnapshot: default
        );

        Assert.Equal(PartyDetailScannerAttemptOutcome.OpenFailed, stateMachine.LastTerminalOutcome);
        Assert.Equal("open_failed", stateMachine.LastAttemptReason);
    }

    [Fact]
    public void State_machine_exposes_typed_success_outcome_when_listing_is_queued() {
        var nowUtc = new DateTime(2026, 4, 15, 1, 0, 0, DateTimeKind.Utc);
        var queue = new DebugPfListingQueue();
        var stateMachine = new DebugPfScanStateMachine(queue);
        var target = new DebugPfListingCandidate(9101U, 55UL, nowUtc, 1);
        stateMachine.UpsertVisibleCandidate(target);

        _ = stateMachine.SyncQueue(
            nowUtc,
            [],
            hasIncomingListings: false,
            maxPerRun: 0,
            dedupTtlSeconds: 600,
            runFromCollectedListings: false
        );

        stateMachine.HandleOpenAttemptResult(
            nowUtc,
            opened: true,
            actionIntervalMs: 400,
            detailReadyTimeoutMs: 3500,
            configuredRetries: 0,
            postListingCooldownMs: 300,
            ackSnapshot: default
        );
        stateMachine.HandleDetailReadyState(
            nowUtc.AddMilliseconds(10),
            new DebugPfDetailSnapshot(target.ListingId, target.ContentId, NonZeroMembers: 4, TotalSlots: 8),
            minDwellMs: 800,
            detailReadyTimeoutMs: 3500,
            configuredRetries: 0,
            postListingCooldownMs: 300
        );
        stateMachine.HandleDetailReadyState(
            nowUtc.AddMilliseconds(20),
            new DebugPfDetailSnapshot(target.ListingId, target.ContentId, NonZeroMembers: 4, TotalSlots: 8),
            minDwellMs: 800,
            detailReadyTimeoutMs: 3500,
            configuredRetries: 0,
            postListingCooldownMs: 300
        );
        stateMachine.HandleCollectedState(
            nowUtc.AddMilliseconds(30),
            new DebugPfCollectorAckSnapshot(
                QueueAckVersion: 1,
                QueuedListingId: target.ListingId,
                SuccessfulAckVersion: 0,
                SuccessfulListingId: 0,
                TerminalAckVersion: 0,
                TerminalListingId: 0
            ),
            postListingCooldownMs: 300
        );

        Assert.Equal(PartyDetailScannerAttemptOutcome.Succeeded, stateMachine.LastTerminalOutcome);
        Assert.Equal("queued", stateMachine.LastAttemptReason);
    }

    [Fact]
    public void BeginScannerRequest_issues_new_request_serial_and_owner() {
        var state = new PartyDetailCaptureState();

        var cycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, listingId: 9001, contentId: 44UL);

        Assert.Equal(1L, cycle.RequestSerial);
        Assert.Equal(PartyDetailRequestOwner.Scanner, cycle.Owner);
        Assert.Equal(9001U, cycle.ListingId);
    }

    [Fact]
    public void RecordArrival_advances_generation_once_per_cycle() {
        var state = new PartyDetailCaptureState();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Manual, 9001U, 44UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));
        Assert.False(state.TryRecordArrival(cycle.RequestSerial, snapshot));
        Assert.Equal(1L, state.LatestArrivalGeneration);
    }

    [Fact]
    public void IsScannerAckReady_rejects_manual_generation_for_scanner_target() {
        var state = new PartyDetailCaptureState();
        var scanner = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var manual = state.BeginRequest(PartyDetailRequestOwner.Manual, 9002U, 55UL);
        var manualSnapshot = CreateSnapshot(9002U, 55UL);

        state.TryRecordArrival(manual.RequestSerial, manualSnapshot);

        Assert.False(state.IsScannerAckReady(scanner.RequestSerial, 9001U, 44UL));
    }

    [Fact]
    public void IsScannerAckReady_accepts_listing_only_scanner_target() {
        var state = new PartyDetailCaptureState();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 0UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));

        Assert.True(state.IsScannerAckReady(cycle.RequestSerial, 9001U, 0UL));
    }

    [Fact]
    public void IsScannerAckReady_rejects_stale_generation_after_consume() {
        var state = new PartyDetailCaptureState();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));
        Assert.True(state.IsScannerAckReady(cycle.RequestSerial, 9001U, 44UL));

        Assert.True(state.TryMarkConsumed(state.LatestArrivalGeneration));
        Assert.Equal(1L, state.LastConsumedGeneration);
        Assert.False(state.IsScannerAckReady(cycle.RequestSerial, 9001U, 44UL));
    }

    [Fact]
    public void TryMarkConsumed_accepts_new_generation_once() {
        var state = new PartyDetailCaptureState();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));

        Assert.True(state.TryMarkConsumed(1L));
        Assert.Equal(1L, state.LastConsumedGeneration);
        Assert.False(state.TryMarkConsumed(1L));
        Assert.False(state.TryMarkConsumed(0L));
    }

    [Fact]
    public void TryRecordArrival_captures_stable_snapshot_before_caller_mutation() {
        var state = new PartyDetailCaptureState();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));

        snapshot.ListingId = 9002U;
        snapshot.LeaderContentId = 55UL;
        snapshot.MemberContentIds[0] = 55UL;

        Assert.True(state.IsScannerAckReady(cycle.RequestSerial, 9001U, 44UL));
    }

    private static UploadablePartyDetail CreateSnapshot(uint listingId, ulong leaderContentId) {
        return new UploadablePartyDetail {
            ListingId = listingId,
            LeaderContentId = leaderContentId,
            MemberContentIds = new List<ulong> { leaderContentId },
        };
    }

    private sealed class FakePartyDetailCaptureRuntime : IDisposable {
        private readonly PartyDetailCaptureRuntime _runtime;

        internal FakePartyDetailCaptureRuntime(PartyDetailCaptureState state) {
            _runtime = new PartyDetailCaptureRuntime(state);
        }

        internal FakePartyDetailCaptureRuntime(
            PartyDetailCaptureState state,
            IPartyDetailCaptureHookFactory hookFactory,
            Action<string> warningSink
        ) {
            _runtime = PartyDetailCaptureRuntime.CreateForTesting(state, hookFactory, warningSink);
        }

        internal void ArmScannerRequest(Guid attemptId, uint listingId, ulong contentId) {
            _runtime.ArmScannerRequest(attemptId, listingId, contentId);
        }

        internal long? GetArmedScannerRequestSerial(Guid attemptId) {
            return _runtime.GetArmedScannerRequestSerial(attemptId);
        }

        internal void BeginScannerOpenAttempt(Guid attemptId) {
            _runtime.BeginScannerOpenAttempt(attemptId);
        }

        internal void EndScannerOpenAttempt(Guid attemptId) {
            _runtime.EndScannerOpenAttempt(attemptId);
        }

        internal void RaiseOpenListing(uint listingId, ulong contentId) {
            _runtime.TestInterceptOpenListing(listingId, contentId);
        }

        internal void RaiseOpenListingByContentId(ulong contentId) {
            _runtime.TestInterceptOpenListingByContentId(contentId);
        }

        internal void CompleteScannerAttempt(Guid attemptId, PartyDetailScannerAttemptOutcome outcome) {
            _runtime.CompleteScannerRequest(attemptId, outcome);
        }

        public void Dispose() {
            // Tests construct the runtime without Dalamud hook initialization.
        }
    }

    private sealed class ThrowingHookFactory : IPartyDetailCaptureHookFactory {
        public IDisposable CreateHooks(
            Func<nint, ulong, bool> openListingDetour,
            Func<nint, ulong, bool> openListingByContentIdDetour
        ) {
            throw new InvalidOperationException("simulated signature drift");
        }
    }

    private sealed class TrackingHook : IDisposable {
        internal int EnableCallCount { get; private set; }
        internal bool IsDisposed { get; private set; }

        internal void Enable() {
            EnableCallCount++;
        }

        public void Dispose() {
            IsDisposed = true;
        }
    }
}
