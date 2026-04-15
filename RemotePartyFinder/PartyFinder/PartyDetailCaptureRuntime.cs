using System;
using Dalamud.Hooking;
using Dalamud.Plugin.Services;

#nullable enable

namespace RemotePartyFinder;

internal enum PartyDetailScannerAttemptOutcome {
    OpenFailed,
    TimedOut,
    Succeeded,
}

internal interface IPartyDetailCaptureHookFactory {
    IDisposable CreateHooks(
        Func<nint, ulong, bool> openListingDetour,
        Func<nint, ulong, bool> openListingByContentIdDetour
    );
}

internal sealed class PartyDetailCaptureRuntime : IDisposable {
    private const string OpenListingSignature =
        "48 89 5C 24 ?? 57 48 83 EC ?? 48 8B FA 48 8B D9 E8 ?? ?? ?? ?? 48 8B 8B ?? ?? ?? ?? 48 85 C9";
    private const string OpenListingByContentIdSignature =
        "40 53 48 83 EC 20 48 8B D9 E8 ?? ?? ?? ?? 84 C0 74 07 C6 83 ?? ?? ?? ?? ?? 48 83 C4 20 5B C3 CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC 40 53";

    private readonly PartyDetailCaptureState _state;
    private readonly object _gate = new();
    private IDisposable? _hookScope;
    private ScannerAttempt? _scannerAttempt;
    private ActiveScannerIntercept? _activeScannerIntercept;

    internal PartyDetailCaptureRuntime(PartyDetailCaptureState state) {
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    internal PartyDetailCaptureRuntime(
        PartyDetailCaptureState state,
        IGameInteropProvider interopProvider,
        Action<string>? warningSink = null
    ) : this(state, new DalamudPartyDetailCaptureHookFactory(interopProvider), warningSink) {
    }

    private PartyDetailCaptureRuntime(
        PartyDetailCaptureState state,
        IPartyDetailCaptureHookFactory hookFactory,
        Action<string>? warningSink = null
    ) : this(state) {
        ArgumentNullException.ThrowIfNull(hookFactory);

        try {
            _hookScope = hookFactory.CreateHooks(OpenListingDetour, OpenListingByContentIdDetour);
        } catch (Exception exception) {
            warningSink?.Invoke($"PartyDetailCaptureRuntime: failed to initialize hooks. {exception.Message}");
            _hookScope = null;
        }
    }

    internal static PartyDetailCaptureRuntime CreateForTesting(
        PartyDetailCaptureState state,
        IPartyDetailCaptureHookFactory hookFactory,
        Action<string>? warningSink = null
    ) {
        return new PartyDetailCaptureRuntime(state, hookFactory, warningSink);
    }

    public void Dispose() {
        _hookScope?.Dispose();
        _hookScope = null;

        lock (_gate) {
            _scannerAttempt = null;
            _activeScannerIntercept = null;
        }
    }

    public void ArmScannerRequest(Guid attemptId, uint listingId, ulong contentId) {
        lock (_gate) {
            if (_scannerAttempt is { AttemptId: var existingAttemptId } existing && existingAttemptId == attemptId) {
                _scannerAttempt = existing with {
                    ListingId = listingId,
                    ContentId = contentId,
                };
                return;
            }

            _scannerAttempt = new ScannerAttempt(attemptId, listingId, contentId, null);
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
        }
    }

    internal void TestInterceptOpenListing(uint listingId, ulong contentId) {
        _ = contentId;
        EnsureRequestCycleForIntercept(listingId, 0UL);
    }

    internal void TestInterceptOpenListingByContentId(ulong contentId) {
        EnsureRequestCycleForIntercept(0U, contentId);
    }

    private bool OpenListingDetour(nint agent, ulong listingId) {
        EnsureRequestCycleForIntercept(NormalizeListingId(listingId), 0UL);
        return true;
    }

    private bool OpenListingByContentIdDetour(nint agent, ulong contentId) {
        EnsureRequestCycleForIntercept(0U, contentId);
        return true;
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

                    _scannerAttempt = attempt with {
                        ListingId = cycle.ListingId,
                        ContentId = cycle.ContentId,
                        RequestSerial = cycle.RequestSerial,
                    };
                }

                return;
            }

            _state.BeginRequest(PartyDetailRequestOwner.Manual, listingId, contentId);
        }
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

    private sealed record ScannerAttempt(Guid AttemptId, uint ListingId, ulong ContentId, long? RequestSerial);
    private sealed record ActiveScannerIntercept(Guid AttemptId, uint ListingId, ulong ContentId);

    private sealed class DalamudPartyDetailCaptureHookFactory : IPartyDetailCaptureHookFactory {
        private readonly IGameInteropProvider _interopProvider;

        private delegate bool OpenListingDelegate(nint agent, ulong listingId);
        private delegate bool OpenListingByContentIdDelegate(nint agent, ulong contentId);

        internal DalamudPartyDetailCaptureHookFactory(IGameInteropProvider interopProvider) {
            _interopProvider = interopProvider ?? throw new ArgumentNullException(nameof(interopProvider));
        }

        public IDisposable CreateHooks(
            Func<nint, ulong, bool> openListingDetour,
            Func<nint, ulong, bool> openListingByContentIdDetour
        ) {
            ArgumentNullException.ThrowIfNull(openListingDetour);
            ArgumentNullException.ThrowIfNull(openListingByContentIdDetour);

            Hook<OpenListingDelegate>? openListingHook = null;
            Hook<OpenListingByContentIdDelegate>? openListingByContentIdHook = null;

            openListingHook = _interopProvider.HookFromSignature<OpenListingDelegate>(
                OpenListingSignature,
                (agent, listingId) => {
                    _ = openListingDetour(agent, listingId);
                    return openListingHook!.Original(agent, listingId);
                }
            );
            openListingByContentIdHook = _interopProvider.HookFromSignature<OpenListingByContentIdDelegate>(
                OpenListingByContentIdSignature,
                (agent, contentId) => {
                    _ = openListingByContentIdDetour(agent, contentId);
                    return openListingByContentIdHook!.Original(agent, contentId);
                }
            );

            openListingHook.Enable();
            openListingByContentIdHook.Enable();
            return new HookScope(openListingHook, openListingByContentIdHook);
        }
    }

    private sealed class HookScope : IDisposable {
        private readonly IDisposable _openListingHook;
        private readonly IDisposable _openListingByContentIdHook;

        internal HookScope(IDisposable openListingHook, IDisposable openListingByContentIdHook) {
            _openListingHook = openListingHook;
            _openListingByContentIdHook = openListingByContentIdHook;
        }

        public void Dispose() {
            _openListingByContentIdHook.Dispose();
            _openListingHook.Dispose();
        }
    }
}
