using System.Collections.Generic;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class DebugPfScannerCaptureTests {
    [Fact]
    public void Scanner_consume_ack_ignores_manual_open_during_run() {
        var state = new PartyDetailCaptureState();
        var scanner = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var manual = state.BeginRequest(PartyDetailRequestOwner.Manual, 9002U, 55UL);
        var manualSnapshot = CreateSnapshot(9002U, 55UL);

        Assert.True(state.TryRecordArrival(manual.RequestSerial, manualSnapshot));
        Assert.True(state.TryMarkConsumed(state.LatestArrivalGeneration));

        Assert.False(state.IsScannerConsumedAckReady(scanner.RequestSerial, 9001U, 44UL));
    }

    [Fact]
    public void Scanner_consume_ack_ignores_stale_late_response_after_retry() {
        var state = new PartyDetailCaptureState();
        var firstCycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var retryCycle = state.BeginRequest(PartyDetailRequestOwner.Scanner, 9001U, 44UL);
        var snapshot = CreateSnapshot(9001U, 44UL);

        Assert.True(state.TryRecordArrival(firstCycle.RequestSerial, snapshot));
        Assert.True(state.TryMarkConsumed(state.LatestArrivalGeneration));

        Assert.False(state.IsScannerConsumedAckReady(retryCycle.RequestSerial, 9001U, 44UL));
    }

    private static UploadablePartyDetail CreateSnapshot(uint listingId, ulong leaderContentId) {
        return new UploadablePartyDetail {
            ListingId = listingId,
            LeaderContentId = leaderContentId,
            MemberContentIds = new List<ulong> { leaderContentId },
        };
    }
}
