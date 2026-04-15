using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class PartyDetailCollectorCaptureTests {
    static PartyDetailCollectorCaptureTests() {
        DalamudAssemblyResolver.Register();
    }

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
    public void Manual_request_never_routes_through_scanner_headless_boundary() {
        using var harness = new ScannerHeadlessBoundaryHarness();
        var snapshot = CreateCompleteSnapshot();

        harness.BeginManualRequest(9001U, 44UL);
        harness.ObserveSharedPopulate();
        harness.Tick(snapshot);
        harness.UpdateCollector();

        Assert.False(harness.ScannerHeadlessBoundaryTouched);
        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, harness.CaptureState.LastConsumedGeneration);
    }

    [Fact]
    public void Scanner_headless_scope_is_disabled_by_default() {
        using var harness = new ScannerHeadlessBoundaryHarness();
        var snapshot = CreateCompleteSnapshot();

        harness.BeginScannerTarget(9001U, 44UL);

        Assert.False(harness.TryBeginScannerHeadlessScope());
        Assert.False(harness.TryRecordScannerHeadlessArrival(snapshot));
        Assert.False(harness.ScannerHeadlessBoundaryTouched);
    }

    [Fact]
    public void Scanner_headless_scope_respects_allow_headless_flag() {
        using var harness = new ScannerHeadlessBoundaryHarness(ScannerHeadlessBackendKind.PopulateCorrelation);

        harness.BeginScannerTarget(9001U, 44UL, allowHeadless: false);

        Assert.False(harness.TryBeginScannerHeadlessScope());
        Assert.False(harness.ScannerHeadlessBoundaryTouched);
    }

    [Fact]
    public void Scanner_headless_arrival_must_reenter_shared_capture_state_and_collector() {
        using var harness = new ScannerHeadlessBoundaryHarness(ScannerHeadlessBackendKind.PopulateCorrelation);
        var snapshot = CreateCompleteSnapshot();

        harness.BeginScannerTarget(9001U, 44UL);
        harness.ObserveSharedPopulate();

        Assert.True(harness.TryBeginScannerHeadlessScope());
        Assert.True(harness.TryRecordScannerHeadlessArrival(snapshot));

        harness.UpdateCollector();

        Assert.True(harness.ScannerHeadlessBoundaryTouched);
        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, harness.CaptureState.LastConsumedGeneration);
    }

    [Fact]
    public void ComposePartyDetailCapture_reuses_single_capture_state_for_runtime_and_collector() {
        var composition = Plugin.ComposePartyDetailCapture(
            plugin: null,
            runtimeFactory: static state => new PartyDetailCaptureRuntime(state),
            collectorFactory: static state => new PartyDetailCollector(
                state,
                tryQueuePayload: static _ => true,
                pumpPendingUploads: static () => { }
            )
        );

        Assert.Same(composition.CaptureState, composition.CaptureRuntime.CaptureState);
        Assert.Same(composition.CaptureState, composition.PartyDetailCollector.CaptureState);
    }

    [Fact]
    public void Reopening_same_listing_in_new_request_cycle_enqueues_again_after_new_population_event_even_if_payload_matches() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var snapshot = CreateCompleteSnapshot();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.ObservePopulationEvent();
        runtime.Tick(snapshot);
        collector.Update();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.ObservePopulationEvent();
        runtime.Tick(snapshot);
        collector.Update();

        Assert.Equal(2, harness.CapturedPayloads.Count);
        Assert.Equal(2L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Reopening_same_listing_does_not_consume_stale_preexisting_snapshot_before_new_population_event() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var snapshot = CreateCompleteSnapshot();

        runtime.Tick(snapshot);
        runtime.BeginManualCycle(9001U, 44UL);

        runtime.Tick(snapshot);
        collector.Update();

        Assert.Empty(harness.CapturedPayloads);
        Assert.Equal(0L, state.LastConsumedGeneration);

        runtime.ObservePopulationEvent();
        collector.Update();

        Assert.Empty(harness.CapturedPayloads);
        Assert.Equal(0L, state.LastConsumedGeneration);

        runtime.Tick(snapshot);
        collector.Update();

        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Valid_snapshot_can_record_without_visibility_state_after_population_event() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();
        var snapshot = CreateCompleteSnapshot();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.ObservePopulationEvent();
        collector.Update();

        Assert.Empty(harness.CapturedPayloads);
        Assert.Equal(0L, state.LastConsumedGeneration);

        runtime.Tick(snapshot);
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
        var completeSnapshot = CreateCompleteSnapshot();

        runtime.BeginManualCycle(9001U, 44UL);
        runtime.ObservePopulationEvent();
        runtime.Tick(incompleteSnapshot);
        collector.Update();

        Assert.Empty(harness.CapturedPayloads);
        Assert.Equal(0L, state.LastConsumedGeneration);

        runtime.Tick(completeSnapshot);
        collector.Update();

        Assert.Single(harness.CapturedPayloads);
        Assert.Equal(1L, state.LastConsumedGeneration);
    }

    [Fact]
    public void Runtime_and_collector_keep_the_same_shared_capture_state_instance() {
        var state = new PartyDetailCaptureState();
        using var runtime = new FakePartyDetailCaptureRuntime(state);
        var harness = new CollectorHarness(state);
        var collector = harness.CreateCollector();

        Assert.Same(state, runtime.CaptureState);
        Assert.Same(state, collector.CaptureState);
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

        internal void ObservePopulationEvent() {
            _runtime.TestObservePopulationEvent();
        }

        internal void Tick(UploadablePartyDetail? snapshot) {
            _runtime.TestFrameworkTick(snapshot);
        }

        internal PartyDetailCaptureState CaptureState => _runtime.CaptureState;

        public void Dispose() {
            _runtime.Dispose();
        }
    }

    private sealed class ScannerHeadlessBoundaryHarness : IDisposable {
        private readonly PartyDetailCaptureRuntime _runtime;
        private readonly PartyDetailCollector _collector;
        private readonly Guid _attemptId = Guid.NewGuid();

        internal ScannerHeadlessBoundaryHarness(
            ScannerHeadlessBackendKind backendKind = ScannerHeadlessBackendKind.Disabled
        ) {
            CaptureState = new PartyDetailCaptureState();
            _runtime = PartyDetailCaptureRuntime.CreateForTesting(CaptureState, backendKind);
            _collector = new PartyDetailCollector(
                CaptureState,
                tryQueuePayload: payload => {
                    CapturedPayloads.Add(CloneCapturedPayload(payload));
                    return true;
                },
                pumpPendingUploads: static () => { }
            );
        }

        internal PartyDetailCaptureState CaptureState { get; }
        internal List<UploadablePartyDetail> CapturedPayloads { get; } = new();
        internal bool ScannerHeadlessBoundaryTouched { get; private set; }

        internal void BeginManualRequest(uint listingId, ulong contentId) {
            _runtime.TestBeginManualRequestCycle(listingId, contentId);
        }

        internal void BeginScannerTarget(uint listingId, ulong contentId, bool allowHeadless = true) {
            _runtime.ArmScannerRequest(_attemptId, listingId, contentId, allowHeadless);
            _runtime.BeginScannerOpenAttempt(_attemptId);
            _runtime.TestInterceptOpenListing(listingId, contentId);
            _runtime.EndScannerOpenAttempt(_attemptId);
        }

        internal void ObserveSharedPopulate() {
            _runtime.TestObservePopulationEvent();
        }

        internal void Tick(UploadablePartyDetail snapshot) {
            _runtime.TestFrameworkTick(snapshot);
        }

        internal bool TryBeginScannerHeadlessScope() {
            var began = _runtime.TryBeginScannerHeadlessScope(_attemptId);
            ScannerHeadlessBoundaryTouched |= began;
            return began;
        }

        internal bool TryRecordScannerHeadlessArrival(UploadablePartyDetail snapshot) {
            var recorded = _runtime.TryRecordScannerHeadlessArrival(_attemptId, snapshot);
            ScannerHeadlessBoundaryTouched |= recorded;
            return recorded;
        }

        internal void UpdateCollector() {
            _collector.Update();
        }

        public void Dispose() {
            _runtime.Dispose();
        }

        private static UploadablePartyDetail CloneCapturedPayload(UploadablePartyDetail payload) {
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

    private static class DalamudAssemblyResolver {
        private static int _registered;

        internal static void Register() {
            if (Interlocked.Exchange(ref _registered, 1) != 0) {
                return;
            }

            AppDomain.CurrentDomain.AssemblyResolve += static (_, args) => {
                var assemblyName = new AssemblyName(args.Name).Name;
                if (string.IsNullOrWhiteSpace(assemblyName)) {
                    return null;
                }

                var dalamudHome = Environment.GetEnvironmentVariable("DALAMUD_HOME");
                if (string.IsNullOrWhiteSpace(dalamudHome)) {
                    dalamudHome = Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                        "XIVLauncher",
                        "addon",
                        "Hooks",
                        "dev"
                    );
                }

                var candidatePath = Path.Combine(dalamudHome, assemblyName + ".dll");
                return File.Exists(candidatePath) ? Assembly.LoadFrom(candidatePath) : null;
            };
        }
    }
}
