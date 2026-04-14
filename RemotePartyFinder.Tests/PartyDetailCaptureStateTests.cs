using System.Collections.Generic;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class PartyDetailCaptureStateTests {
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
    public void ConsumeAck_rejects_stale_or_manual_generation_for_scanner_target() {
        var state = new PartyDetailCaptureState();
        var scanner = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var manual = state.BeginRequest(PartyDetailRequestOwner.Manual, 9002U, 55UL);
        var manualSnapshot = CreateSnapshot(9002U, 55UL);

        state.TryRecordArrival(manual.RequestSerial, manualSnapshot);

        Assert.False(state.IsScannerAckReady(scanner.RequestSerial, 9001U, 44UL));
    }

    [Fact]
    public void MarkConsumed_accepts_new_generation_once() {
        var state = new PartyDetailCaptureState();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));

        Assert.True(state.TryMarkConsumed(1L));
        Assert.False(state.TryMarkConsumed(1L));
        Assert.False(state.TryMarkConsumed(0L));
    }

    private static UploadablePartyDetail CreateSnapshot(uint listingId, ulong leaderContentId) {
        return new UploadablePartyDetail {
            ListingId = listingId,
            LeaderContentId = leaderContentId,
            MemberContentIds = new List<ulong> { leaderContentId },
        };
    }
}
