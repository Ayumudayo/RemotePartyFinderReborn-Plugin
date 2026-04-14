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
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);

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
        runtime.RaiseOpenListing(listingId: 9001U, contentId: 44UL);
        var firstRequestSerial = state.CurrentRequestSerial;

        runtime.RaiseOpenListingByContentId(contentId: 44UL);

        Assert.Equal(firstRequestSerial, state.CurrentRequestSerial);
        Assert.Equal(firstRequestSerial, runtime.GetArmedScannerRequestSerial(attemptId));
        Assert.Equal(PartyDetailRequestOwner.Scanner, state.CurrentOwner);
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

        internal void ArmScannerRequest(Guid attemptId, uint listingId, ulong contentId) {
            _runtime.ArmScannerRequest(attemptId, listingId, contentId);
        }

        internal long? GetArmedScannerRequestSerial(Guid attemptId) {
            return _runtime.GetArmedScannerRequestSerial(attemptId);
        }

        internal void RaiseOpenListing(uint listingId, ulong contentId) {
            _runtime.TestInterceptOpenListing(listingId, contentId);
        }

        internal void RaiseOpenListingByContentId(ulong contentId) {
            _runtime.TestInterceptOpenListingByContentId(contentId);
        }

        public void Dispose() {
            // Tests construct the runtime without Dalamud hook initialization.
        }
    }
}
