using System;
using System.Collections.Generic;
using Dalamud.Hooking;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;

#nullable enable

namespace RemotePartyFinder;

internal enum PartyDetailScannerAttemptOutcome {
    OpenFailed,
    TimedOut,
    Succeeded,
}

internal enum ScannerHeadlessBackendKind {
    Disabled,
    NativeSuppression,
    PopulateCorrelation,
}

internal interface IPartyDetailCaptureHookFactory {
    IDisposable CreateHooks(
        Func<nint, ulong, bool> openListingDetour,
        Func<nint, ulong, bool> openListingByContentIdDetour,
        Action<nint, nint> populateListingDataDetour
    );
}

internal sealed class PartyDetailCaptureRuntime : IDisposable {
    private const string OpenListingSignature =
        "48 89 5C 24 ?? 57 48 83 EC ?? 48 8B FA 48 8B D9 E8 ?? ?? ?? ?? 48 8B 8B ?? ?? ?? ?? 48 85 C9";
    private const string OpenListingByContentIdSignature =
        "40 53 48 83 EC 20 48 8B D9 E8 ?? ?? ?? ?? 84 C0 74 07 C6 83 ?? ?? ?? ?? ?? 48 83 C4 20 5B C3 CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC 40 53";
    private readonly PartyDetailCaptureState _state;
    private readonly object _gate = new();
    private readonly ScannerHeadlessBackendKind _scannerHeadlessBackendKind;
    private IDisposable? _hookScope;
    private ScannerAttempt? _scannerAttempt;
    private ActiveScannerIntercept? _activeScannerIntercept;
    private ActiveScannerHeadlessScope? _activeScannerHeadlessScope;
    private long _postRequestPopulationGeneration;
    private RequestPopulationGate? _requestPopulationGate;
    internal PartyDetailCaptureState CaptureState => _state;

    // Scanner-owned PF detail UI suppression remains deferred. The shared
    // PopulateCorrelation boundary is in place for scanner-owned arrivals, but
    // the current PF-specific addon opener candidate (sub_14053DE30) is too
    // broad because it mutates agent/UI state around PopulateListingData in
    // addition to opening the detail addon.

    internal PartyDetailCaptureRuntime(
        PartyDetailCaptureState state,
        ScannerHeadlessBackendKind scannerHeadlessBackendKind = ScannerHeadlessBackendKind.Disabled
    ) {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _scannerHeadlessBackendKind = scannerHeadlessBackendKind;
    }

    internal PartyDetailCaptureRuntime(
        PartyDetailCaptureState state,
        IGameInteropProvider interopProvider,
        Action<string>? warningSink = null,
        ScannerHeadlessBackendKind scannerHeadlessBackendKind = ScannerHeadlessBackendKind.Disabled
    ) : this(state, new DalamudPartyDetailCaptureHookFactory(interopProvider), warningSink, scannerHeadlessBackendKind) {
    }

    private PartyDetailCaptureRuntime(
        PartyDetailCaptureState state,
        IPartyDetailCaptureHookFactory hookFactory,
        Action<string>? warningSink = null,
        ScannerHeadlessBackendKind scannerHeadlessBackendKind = ScannerHeadlessBackendKind.Disabled
    ) : this(state, scannerHeadlessBackendKind) {
        ArgumentNullException.ThrowIfNull(hookFactory);

        try {
            _hookScope = hookFactory.CreateHooks(OpenListingDetour, OpenListingByContentIdDetour, PopulateListingDataDetour);
        } catch (Exception exception) {
            warningSink?.Invoke($"PartyDetailCaptureRuntime: failed to initialize hooks. {exception.Message}");
            _hookScope = null;
        }
    }

    internal static PartyDetailCaptureRuntime CreateForTesting(
        PartyDetailCaptureState state,
        ScannerHeadlessBackendKind scannerHeadlessBackendKind
    ) {
        return new PartyDetailCaptureRuntime(state, scannerHeadlessBackendKind);
    }

    internal static PartyDetailCaptureRuntime CreateForTesting(
        PartyDetailCaptureState state,
        IPartyDetailCaptureHookFactory hookFactory,
        Action<string>? warningSink = null,
        ScannerHeadlessBackendKind scannerHeadlessBackendKind = ScannerHeadlessBackendKind.Disabled
    ) {
        return new PartyDetailCaptureRuntime(state, hookFactory, warningSink, scannerHeadlessBackendKind);
    }

    internal static IDisposable CreateHookScope<TPrimaryHook, TSecondaryHook>(
        Func<TPrimaryHook> createPrimaryHook,
        Func<TSecondaryHook> createSecondaryHook,
        Action<TPrimaryHook> enablePrimaryHook,
        Action<TSecondaryHook> enableSecondaryHook
    )
        where TPrimaryHook : class, IDisposable
        where TSecondaryHook : class, IDisposable {
        ArgumentNullException.ThrowIfNull(createPrimaryHook);
        ArgumentNullException.ThrowIfNull(createSecondaryHook);
        ArgumentNullException.ThrowIfNull(enablePrimaryHook);
        ArgumentNullException.ThrowIfNull(enableSecondaryHook);

        TPrimaryHook? primaryHook = null;
        TSecondaryHook? secondaryHook = null;

        try {
            primaryHook = createPrimaryHook();
            secondaryHook = createSecondaryHook();
            enablePrimaryHook(primaryHook);
            enableSecondaryHook(secondaryHook);
            return new HookScope(primaryHook, secondaryHook);
        } catch {
            secondaryHook?.Dispose();
            primaryHook?.Dispose();
            throw;
        }
    }

    public void Dispose() {
        _hookScope?.Dispose();
        _hookScope = null;

        lock (_gate) {
            _scannerAttempt = null;
            _activeScannerIntercept = null;
            _activeScannerHeadlessScope = null;
            _requestPopulationGate = null;
        }
    }

    public void ArmScannerRequest(Guid attemptId, uint listingId, ulong contentId, bool allowHeadless = true) {
        lock (_gate) {
            if (_scannerAttempt is { AttemptId: var existingAttemptId } existing && existingAttemptId == attemptId) {
                _scannerAttempt = existing with {
                    ListingId = listingId,
                    ContentId = contentId,
                    AllowHeadless = allowHeadless,
                };
                return;
            }

            _scannerAttempt = new ScannerAttempt(attemptId, listingId, contentId, null, allowHeadless);
        }
    }

    public long? GetArmedScannerRequestSerial(Guid attemptId) {
        lock (_gate) {
            return _scannerAttempt is { AttemptId: var armedAttemptId } attempt && armedAttemptId == attemptId
                ? attempt.RequestSerial
                : null;
        }
    }

    internal void BeginScannerOpenAttempt(Guid attemptId) {
        lock (_gate) {
            if (_scannerAttempt is not { AttemptId: var armedAttemptId } attempt || armedAttemptId != attemptId) {
                return;
            }

            _activeScannerIntercept = new ActiveScannerIntercept(attempt.AttemptId, attempt.ListingId, attempt.ContentId);
        }
    }

    internal void EndScannerOpenAttempt(Guid attemptId) {
        lock (_gate) {
            if (_activeScannerIntercept is { AttemptId: var activeAttemptId } && activeAttemptId == attemptId) {
                _activeScannerIntercept = null;
            }
        }
    }

    internal void ClearScannerRequest(Guid attemptId) {
        lock (_gate) {
            if (_scannerAttempt is { AttemptId: var armedAttemptId } && armedAttemptId == attemptId) {
                _scannerAttempt = null;
            }

            if (_activeScannerIntercept is { AttemptId: var activeAttemptId } && activeAttemptId == attemptId) {
                _activeScannerIntercept = null;
            }

            if (_activeScannerHeadlessScope is { AttemptId: var activeHeadlessAttemptId } && activeHeadlessAttemptId == attemptId) {
                _activeScannerHeadlessScope = null;
            }
        }
    }

    internal void CompleteScannerRequest(Guid attemptId, PartyDetailScannerAttemptOutcome outcome) {
        if (ShouldClearScannerRequest(outcome)) {
            ClearScannerRequest(attemptId);
        }
    }

    internal void ResetScannerRequest() {
        lock (_gate) {
            _scannerAttempt = null;
            _activeScannerIntercept = null;
            _activeScannerHeadlessScope = null;
            _requestPopulationGate = null;
        }
    }

    internal bool TryBeginScannerHeadlessScope(Guid attemptId) {
        if (_scannerHeadlessBackendKind != ScannerHeadlessBackendKind.PopulateCorrelation) {
            return false;
        }

        lock (_gate) {
            if (_activeScannerHeadlessScope is { AttemptId: var activeAttemptId } activeScope && activeAttemptId == attemptId) {
                return true;
            }

            if (_scannerAttempt is not { AttemptId: var armedAttemptId, RequestSerial: var requestSerial, AllowHeadless: true } attempt
                || armedAttemptId != attemptId
                || !requestSerial.HasValue) {
                return false;
            }

            if (!_state.TryGetCurrentRequestCycle(out var cycle)
                || cycle.RequestSerial != requestSerial.Value
                || cycle.Owner != PartyDetailRequestOwner.Scanner) {
                return false;
            }

            _activeScannerHeadlessScope = new ActiveScannerHeadlessScope(
                attemptId,
                cycle.RequestSerial,
                cycle.ListingId,
                cycle.ContentId
            );
            return true;
        }
    }

    internal void EndScannerHeadlessScope(Guid attemptId) {
        lock (_gate) {
            if (_activeScannerHeadlessScope is { AttemptId: var activeAttemptId } && activeAttemptId == attemptId) {
                _activeScannerHeadlessScope = null;
            }
        }
    }

    internal bool TryRecordScannerHeadlessArrival(Guid attemptId, UploadablePartyDetail snapshot) {
        ArgumentNullException.ThrowIfNull(snapshot);

        if (_scannerHeadlessBackendKind != ScannerHeadlessBackendKind.PopulateCorrelation) {
            return false;
        }

        ActiveScannerHeadlessScope? scope;
        lock (_gate) {
            if (_activeScannerHeadlessScope is not { AttemptId: var activeAttemptId } activeScope || activeAttemptId != attemptId) {
                return false;
            }

            scope = activeScope;
        }

        if (!_state.TryGetCurrentRequestCycle(out var cycle)
            || cycle.RequestSerial != scope.RequestSerial
            || cycle.Owner != PartyDetailRequestOwner.Scanner) {
            return false;
        }

        if (!HasObservedPostRequestPopulation(scope.RequestSerial)) {
            return false;
        }

        if (!PartyDetailCollector.IsSnapshotReadyForEnqueue(snapshot)) {
            return false;
        }

        if (scope.ListingId != 0 && snapshot.ListingId != scope.ListingId) {
            return false;
        }

        if (scope.ContentId != 0 && snapshot.LeaderContentId != scope.ContentId) {
            return false;
        }

        if (!_state.TryRecordArrival(scope.RequestSerial, snapshot)) {
            return false;
        }

        ClearPopulationGate(scope.RequestSerial);
        EndScannerHeadlessScope(attemptId);
        return true;
    }

    internal void TestInterceptOpenListing(uint listingId, ulong contentId) {
        _ = contentId;
        EnsureRequestCycleForIntercept(listingId, 0UL);
    }

    internal void TestInterceptOpenListingByContentId(ulong contentId) {
        EnsureRequestCycleForIntercept(0U, contentId);
    }

    internal void TestBeginManualRequestCycle(uint listingId, ulong contentId) {
        lock (_gate) {
            BeginManualRequestCycle(listingId, contentId);
        }
    }

    internal void OnFrameworkUpdate() {
        ObserveFrameworkTick(TryBuildSnapshotFromAgent(out var snapshot) ? snapshot : null);
    }

    internal void TestObservePopulationEvent() {
        RaisePostRequestPopulationSignal();
    }

    internal void TestFrameworkTick(UploadablePartyDetail? snapshot) {
        ObserveFrameworkTick(snapshot);
    }

    private bool OpenListingDetour(nint agent, ulong listingId) {
        EnsureRequestCycleForIntercept(NormalizeListingId(listingId), 0UL);
        return true;
    }

    private bool OpenListingByContentIdDetour(nint agent, ulong contentId) {
        EnsureRequestCycleForIntercept(0U, contentId);
        return true;
    }

    private void PopulateListingDataDetour(nint agent, nint listingData) {
        _ = agent;
        _ = listingData;
        RaisePostRequestPopulationSignal();
    }

    private void EnsureRequestCycleForIntercept(uint listingId, ulong contentId) {
        lock (_gate) {
            if (_scannerAttempt is { } attempt
                && _activeScannerIntercept is { } activeIntercept
                && activeIntercept.AttemptId == attempt.AttemptId
                && IsCompatible(activeIntercept.ListingId, activeIntercept.ContentId, listingId, contentId)) {
                if (!attempt.RequestSerial.HasValue) {
                    var cycle = _state.BeginRequest(
                        PartyDetailRequestOwner.Scanner,
                        attempt.ListingId != 0 ? attempt.ListingId : listingId,
                        attempt.ContentId != 0 ? attempt.ContentId : contentId
                    );
                    ArmPopulationGate(cycle.RequestSerial);

                    _scannerAttempt = attempt with {
                        ListingId = cycle.ListingId,
                        ContentId = cycle.ContentId,
                        RequestSerial = cycle.RequestSerial,
                    };
                }

                return;
            }

            BeginManualRequestCycle(listingId, contentId);
        }
    }

    private void BeginManualRequestCycle(uint listingId, ulong contentId) {
        var cycle = _state.BeginRequest(PartyDetailRequestOwner.Manual, listingId, contentId);
        ArmPopulationGate(cycle.RequestSerial);
    }

    private void ObserveFrameworkTick(UploadablePartyDetail? snapshot) {
        if (!_state.HasActiveRequest) {
            return;
        }

        if (snapshot is null) {
            return;
        }

        TryRecordArrivalFromAgentSnapshotCore(snapshot, enforceFreshness: true);
    }

    private bool TryRecordArrivalFromAgentSnapshotCore(UploadablePartyDetail snapshot, bool enforceFreshness) {
        if (!_state.TryGetCurrentRequestCycle(out var cycle)) {
            return false;
        }

        if (enforceFreshness && !HasObservedPostRequestPopulation(cycle.RequestSerial)) {
            return false;
        }

        if (!PartyDetailCollector.IsSnapshotReadyForEnqueue(snapshot)) {
            return false;
        }

        if (cycle.ListingId != 0 && snapshot.ListingId != cycle.ListingId) {
            return false;
        }

        if (cycle.ContentId != 0 && snapshot.LeaderContentId != cycle.ContentId) {
            return false;
        }

        if (!_state.TryRecordArrival(cycle.RequestSerial, snapshot)) {
            return false;
        }

        ClearPopulationGate(cycle.RequestSerial);
        return true;
    }

    private static unsafe bool TryBuildSnapshotFromAgent(out UploadablePartyDetail snapshot) {
        snapshot = new UploadablePartyDetail();

        var lookingForGroupAgent = AgentLookingForGroup.Instance();
        if (lookingForGroupAgent == null) {
            return false;
        }

        ref var viewedListing = ref lookingForGroupAgent->LastViewedListing;
        if (viewedListing.ListingId == 0) {
            return false;
        }

        var effectiveParties = Math.Max(1, (int)viewedListing.NumberOfParties);
        var declaredSlots = Math.Max((int)viewedListing.TotalSlots, effectiveParties * 8);
        var slotCount = Math.Clamp(declaredSlots, 0, 48);
        if (slotCount <= 0) {
            return false;
        }

        var memberContentIds = new List<ulong>(slotCount);
        var memberJobs = new List<byte>(slotCount);
        var slotFlags = new List<string>(slotCount);

        for (var slotIndex = 0; slotIndex < slotCount; slotIndex++) {
            memberContentIds.Add(viewedListing.MemberContentIds[slotIndex]);
            memberJobs.Add(viewedListing.Jobs[slotIndex]);
            var rawSlotFlag = Convert.ToUInt64(viewedListing.SlotFlags[slotIndex]);
            slotFlags.Add($"0x{rawSlotFlag:X16}");
        }

        snapshot = new UploadablePartyDetail {
            ListingId = viewedListing.ListingId,
            LeaderContentId = viewedListing.LeaderContentId,
            LeaderName = lookingForGroupAgent->LastLeader.ToString(),
            HomeWorld = viewedListing.HomeWorld,
            MemberContentIds = memberContentIds,
            MemberJobs = memberJobs,
            SlotFlags = slotFlags,
        };

        return true;
    }

    private static uint NormalizeListingId(ulong listingId) {
        return listingId <= uint.MaxValue ? (uint)listingId : 0U;
    }

    private static bool IsCompatible(uint armedListingId, ulong armedContentId, uint listingId, ulong contentId) {
        if (armedListingId != 0 && listingId != 0 && armedListingId != listingId) {
            return false;
        }

        if (armedContentId != 0 && contentId != 0 && armedContentId != contentId) {
            return false;
        }

        return true;
    }

    private static bool ShouldClearScannerRequest(PartyDetailScannerAttemptOutcome outcome) {
        return outcome switch {
            PartyDetailScannerAttemptOutcome.OpenFailed => false,
            PartyDetailScannerAttemptOutcome.TimedOut => true,
            PartyDetailScannerAttemptOutcome.Succeeded => true,
            _ => false,
        };
    }

    private void ArmPopulationGate(long requestSerial) {
        lock (_gate) {
            _requestPopulationGate = new RequestPopulationGate(requestSerial, _postRequestPopulationGeneration);
        }
    }

    private bool HasObservedPostRequestPopulation(long requestSerial) {
        lock (_gate) {
            return _requestPopulationGate is not { } gate
                   || gate.RequestSerial != requestSerial
                   || _postRequestPopulationGeneration > gate.SignalGenerationAtRequestStart;
        }
    }

    private void ClearPopulationGate(long requestSerial) {
        lock (_gate) {
            if (_requestPopulationGate is { RequestSerial: var gatedRequestSerial } && gatedRequestSerial == requestSerial) {
                _requestPopulationGate = null;
            }
        }
    }

    private void RaisePostRequestPopulationSignal() {
        lock (_gate) {
            _postRequestPopulationGeneration++;
        }
    }

    private sealed record ScannerAttempt(Guid AttemptId, uint ListingId, ulong ContentId, long? RequestSerial, bool AllowHeadless);
    private sealed record ActiveScannerIntercept(Guid AttemptId, uint ListingId, ulong ContentId);
    private sealed record ActiveScannerHeadlessScope(Guid AttemptId, long RequestSerial, uint ListingId, ulong ContentId);
    private sealed record RequestPopulationGate(long RequestSerial, long SignalGenerationAtRequestStart);

    private sealed class DalamudPartyDetailCaptureHookFactory : IPartyDetailCaptureHookFactory {
        private readonly IGameInteropProvider _interopProvider;

        private delegate bool OpenListingDelegate(nint agent, ulong listingId);
        private delegate bool OpenListingByContentIdDelegate(nint agent, ulong contentId);
        private delegate void PopulateListingDataDelegate(nint agent, nint listingData);

        internal DalamudPartyDetailCaptureHookFactory(IGameInteropProvider interopProvider) {
            _interopProvider = interopProvider ?? throw new ArgumentNullException(nameof(interopProvider));
        }

        public unsafe IDisposable CreateHooks(
            Func<nint, ulong, bool> openListingDetour,
            Func<nint, ulong, bool> openListingByContentIdDetour,
            Action<nint, nint> populateListingDataDetour
        ) {
            ArgumentNullException.ThrowIfNull(openListingDetour);
            ArgumentNullException.ThrowIfNull(openListingByContentIdDetour);
            ArgumentNullException.ThrowIfNull(populateListingDataDetour);

            Hook<OpenListingDelegate>? openListingHook = null;
            Hook<OpenListingByContentIdDelegate>? openListingByContentIdHook = null;
            Hook<PopulateListingDataDelegate>? populateListingDataHook = null;
            IDisposable? openListingHookScope = null;

            try {
                openListingHookScope = CreateHookScope(
                    createPrimaryHook: () =>
                        openListingHook = _interopProvider.HookFromSignature<OpenListingDelegate>(
                            OpenListingSignature,
                            (agent, listingId) => {
                                _ = openListingDetour(agent, listingId);
                                return openListingHook!.Original(agent, listingId);
                            }
                        ),
                    createSecondaryHook: () =>
                        openListingByContentIdHook = _interopProvider.HookFromSignature<OpenListingByContentIdDelegate>(
                            OpenListingByContentIdSignature,
                            (agent, contentId) => {
                                _ = openListingByContentIdDetour(agent, contentId);
                                return openListingByContentIdHook!.Original(agent, contentId);
                            }
                        ),
                    enablePrimaryHook: static hook => hook.Enable(),
                    enableSecondaryHook: static hook => hook.Enable()
                );

                populateListingDataHook = _interopProvider.HookFromAddress<PopulateListingDataDelegate>(
                    (nint)AgentLookingForGroup.MemberFunctionPointers.PopulateListingData,
                    (agent, listingData) => {
                        populateListingDataHook!.Original(agent, listingData);
                        populateListingDataDetour(agent, listingData);
                    }
                );
                populateListingDataHook.Enable();
                return new HookScope(openListingHookScope, populateListingDataHook);
            } catch {
                populateListingDataHook?.Dispose();
                openListingHookScope?.Dispose();
                throw;
            }
        }
    }

    private sealed class HookScope : IDisposable {
        private readonly IDisposable _openListingHookScope;
        private readonly IDisposable _populateListingDataHook;

        internal HookScope(IDisposable openListingHookScope, IDisposable populateListingDataHook) {
            _openListingHookScope = openListingHookScope;
            _populateListingDataHook = populateListingDataHook;
        }

        public void Dispose() {
            _populateListingDataHook.Dispose();
            _openListingHookScope.Dispose();
        }
    }
}
