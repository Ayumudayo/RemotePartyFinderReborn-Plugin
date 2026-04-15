using System;
using System.Collections.Generic;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class PartyDetailCollectorCaptureTests {
    [Fact]
    public void Update_enqueues_only_once_for_same_arrival_generation() {
        var state = new PartyDetailCaptureState();
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Manual, 9001U, 44UL);
        var snapshot = CreateCompleteSnapshot();

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));

        collector.Update();
        collector.Update();

        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Update_retries_same_generation_when_enqueue_fails() {
        var state = new PartyDetailCaptureState();
        var harness = new CollectorHarness(state, failFirstEnqueue: true);
        var collector = harness.CreateCollector();
        var cycle = state.BeginRequest(PartyDetailRequestOwner.Manual, 9001U, 44UL);
        var snapshot = CreateCompleteSnapshot();

        Assert.True(state.TryRecordArrival(cycle.RequestSerial, snapshot));

        collector.Update();
        collector.Update();

        Assert.Equal(2, harness.EnqueueAttempts);
        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Reopening_same_listing_in_new_request_cycle_enqueues_again_even_if_payload_matches() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var snapshot = CreateCompleteSnapshot();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.RecordArrivalFromAgent(snapshot);
        collector.Update();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.RecordArrivalFromAgent(snapshot);
        collector.Update();

        Assert.Equal(2, harness.CapturedPayloads.Count);
        Assert.Equal(2L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Reopening_same_listing_does_not_consume_stale_preexisting_snapshot_before_fresh_observation() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var snapshot = CreateCompleteSnapshot();

        runtime.ObserveAgentSnapshot(snapshot);
        runtime.BeginManualCycle(9001U, 44UL);

        runtime.ObserveAgentSnapshot(snapshot);
        collector.Update();

        Assert.Empty(harness.CapturedPayloads);
        Assert.Equal(0L, state.LastConsumedGeneration);

        runtime.ObserveNoAgentSnapshot();
        runtime.ObserveAgentSnapshot(snapshot);
        collector.Update();

        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Incomplete_snapshot_is_not_consumed_and_retries_on_next_tick() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var incompleteSnapshot = CreateIncompleteSnapshot();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.RecordArrivalFromAgent(incompleteSnapshot);
        collector.Update();
        collector.Update();

        Assert.Empty(harness.CapturedPayloads);
        Assert.Equal(0L, state.LastConsumedGeneration);
    }

    private static UploadablePartyDetail CreateCompleteSnapshot() {
        return new UploadablePartyDetail {
            ListingId = 9001U,
            LeaderContentId = 44UL,
            LeaderName = "Leader",
            HomeWorld = 77,
            MemberContentIds = [44UL, 55UL],
            MemberJobs = [19, 24],
            SlotFlags = ["0x0000000000000000", "0x0000000000000000"],
        };
    }

    private static UploadablePartyDetail CreateIncompleteSnapshot() {
        return new UploadablePartyDetail {
            ListingId = 9001U,
            LeaderContentId = 44UL,
            LeaderName = "Leader",
            HomeWorld = 77,
            MemberContentIds = [0UL, 0UL],
            MemberJobs = [19, 24],
            SlotFlags = ["0x0000000000000000", "0x0000000000000000"],
        };
    }

    private sealed class CollectorHarness {
        private readonly PartyDetailCaptureState _state;
        private readonly bool _failFirstEnqueue;

        internal CollectorHarness(PartyDetailCaptureState state, bool failFirstEnqueue = false) {
            _state = state;
            _failFirstEnqueue = failFirstEnqueue;
        }

        internal List<UploadablePartyDetail> CapturedPayloads { get; } = new();
        internal int EnqueueAttempts { get; private set; }

        internal PartyDetailCollector CreateCollector() {
            return new PartyDetailCollector(
                _state,
                tryQueuePayload: payload => {
                    EnqueueAttempts++;
                    if (_failFirstEnqueue && EnqueueAttempts == 1) {
                        return false;
                    }

                    CapturedPayloads.Add(ClonePayload(payload));
                    return true;
                },
                pumpPendingUploads: static () => { }
            );
        }

        private static UploadablePartyDetail ClonePayload(UploadablePartyDetail payload) {
            return new UploadablePartyDetail {
                ListingId = payload.ListingId,
                LeaderContentId = payload.LeaderContentId,
                LeaderName = payload.LeaderName,
                HomeWorld = payload.HomeWorld,
                MemberContentIds = [.. payload.MemberContentIds],
                MemberJobs = [.. payload.MemberJobs],
                SlotFlags = [.. payload.SlotFlags],
            };
        }
    }

    private sealed class FakePartyDetailCaptureRuntime : IDisposable {
        private readonly PartyDetailCaptureState _state;
        private readonly PartyDetailCaptureRuntime _runtime;

        internal FakePartyDetailCaptureRuntime(PartyDetailCaptureState state) {
            _state = state;
            _runtime = new PartyDetailCaptureRuntime(state);
        }

        internal void BeginManualCycle(uint listingId, ulong contentId) {
            _runtime.TestBeginManualRequestCycle(listingId, contentId);
        }

        internal void RecordArrivalFromAgent(UploadablePartyDetail snapshot) {
            _runtime.TestRecordArrivalFromAgentSnapshot(snapshot);
        }

        internal void ObserveAgentSnapshot(UploadablePartyDetail snapshot) {
            _runtime.TestObserveAgentSnapshot(snapshot);
        }

        internal void ObserveNoAgentSnapshot() {
            _runtime.TestObserveAgentSnapshot(null);
        }

        public void Dispose() {
            _runtime.Dispose();
        }
    }
}
