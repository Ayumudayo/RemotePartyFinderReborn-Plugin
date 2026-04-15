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

    [Fact]
    public void Scanner_owned_request_without_headless_suppression_still_captures_once_per_request_cycle() {
        using var harness = new ScannerOwnedCaptureHarness();
        var snapshot = CreateSnapshot(9001U, 44UL);

        harness.BeginScannerTarget(9001U, 44UL);
        harness.ObservePopulationEvent();
        harness.Tick(snapshot);
        harness.UpdateCollector();

        harness.Tick(snapshot);
        harness.UpdateCollector();

        Assert.Single(harness.CapturedPayloads);
    }

    private static UploadablePartyDetail CreateSnapshot(uint listingId, ulong leaderContentId) {
        return new UploadablePartyDetail {
            ListingId = listingId,
            LeaderContentId = leaderContentId,
            LeaderName = "Leader",
            HomeWorld = 77,
            MemberContentIds = new List<ulong> { leaderContentId },
            MemberJobs = new List<byte> { 19 },
            SlotFlags = new List<string> { "0x0000000000000000" },
        };
    }

    private sealed class ScannerOwnedCaptureHarness : IDisposable {
        private readonly PartyDetailCaptureRuntime _runtime;
        private readonly PartyDetailCollector _collector;
        private readonly Guid _attemptId = Guid.NewGuid();

        internal ScannerOwnedCaptureHarness() {
            var state = new PartyDetailCaptureState();
            _runtime = new PartyDetailCaptureRuntime(state);
            _collector = new PartyDetailCollector(
                state,
                tryQueuePayload: payload => {
                    CapturedPayloads.Add(CloneSnapshot(payload));
                    return true;
                },
                pumpPendingUploads: static () => { }
            );
        }

        internal List<UploadablePartyDetail> CapturedPayloads { get; } = new();

        internal void BeginScannerTarget(uint listingId, ulong contentId) {
            _runtime.ArmScannerRequest(_attemptId, listingId, contentId);
            _runtime.BeginScannerOpenAttempt(_attemptId);
            _runtime.TestInterceptOpenListing(listingId, contentId);
            _runtime.EndScannerOpenAttempt(_attemptId);
        }

        internal void ObservePopulationEvent() {
            _runtime.TestObservePopulationEvent();
        }

        internal void Tick(UploadablePartyDetail snapshot) {
            _runtime.TestFrameworkTick(snapshot);
        }

        internal void UpdateCollector() {
            _collector.Update();
        }

        public void Dispose() {
            _runtime.Dispose();
        }

        private static UploadablePartyDetail CloneSnapshot(UploadablePartyDetail payload) {
            return new UploadablePartyDetail {
                ListingId = payload.ListingId,
                LeaderContentId = payload.LeaderContentId,
                LeaderName = payload.LeaderName,
                HomeWorld = payload.HomeWorld,
                MemberContentIds = new List<ulong>(payload.MemberContentIds),
                MemberJobs = new List<byte>(payload.MemberJobs),
                SlotFlags = new List<string>(payload.SlotFlags),
            };
        }
    }
}
