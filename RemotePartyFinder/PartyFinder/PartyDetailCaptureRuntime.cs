using System;
using Dalamud.Hooking;
using Dalamud.Plugin.Services;

#nullable enable

namespace RemotePartyFinder;

internal sealed class PartyDetailCaptureRuntime : IDisposable {
    private const string OpenListingSignature =
        "48 89 5C 24 ?? 57 48 83 EC ?? 48 8B FA 48 8B D9 E8 ?? ?? ?? ?? 48 8B 8B ?? ?? ?? ?? 48 85 C9";
    private const string OpenListingByContentIdSignature =
        "40 53 48 83 EC 20 48 8B D9 E8 ?? ?? ?? ?? 84 C0 74 07 C6 83 ?? ?? ?? ?? ?? 48 83 C4 20 5B C3 CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC CC 40 53";

    private readonly PartyDetailCaptureState _state;
    private readonly object _gate = new();
    private Hook<OpenListingDelegate>? _openListingHook;
    private Hook<OpenListingByContentIdDelegate>? _openListingByContentIdHook;
    private ScannerArm? _currentScannerArm;

    private delegate bool OpenListingDelegate(nint agent, ulong listingId);
    private delegate bool OpenListingByContentIdDelegate(nint agent, ulong contentId);

    internal PartyDetailCaptureRuntime(PartyDetailCaptureState state) {
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    internal PartyDetailCaptureRuntime(
        PartyDetailCaptureState state,
        IGameInteropProvider interopProvider,
        Action<string>? warningSink = null
    ) : this(state) {
        ArgumentNullException.ThrowIfNull(interopProvider);

        try {
            _openListingHook = interopProvider.HookFromSignature<OpenListingDelegate>(
                OpenListingSignature,
                OpenListingDetour
            );
            _openListingByContentIdHook = interopProvider.HookFromSignature<OpenListingByContentIdDelegate>(
                OpenListingByContentIdSignature,
                OpenListingByContentIdDetour
            );
            _openListingHook.Enable();
            _openListingByContentIdHook.Enable();
        } catch (Exception exception) {
            warningSink?.Invoke($"PartyDetailCaptureRuntime: failed to initialize hooks. {exception.Message}");
            Dispose();
            throw;
        }
    }

    public void Dispose() {
        _openListingHook?.Dispose();
        _openListingHook = null;
        _openListingByContentIdHook?.Dispose();
        _openListingByContentIdHook = null;

        lock (_gate) {
            _currentScannerArm = null;
        }
    }

    public void ArmScannerRequest(Guid attemptId, uint listingId, ulong contentId) {
        lock (_gate) {
            if (_currentScannerArm is { AttemptId: var armedAttemptId } existing && armedAttemptId == attemptId) {
                _currentScannerArm = existing with {
                    ListingId = listingId,
                    ContentId = contentId,
                };
                return;
            }

            _currentScannerArm = new ScannerArm(attemptId, listingId, contentId, null);
        }
    }

    public long? GetArmedScannerRequestSerial(Guid attemptId) {
        lock (_gate) {
            return _currentScannerArm is { AttemptId: var armedAttemptId } arm && armedAttemptId == attemptId
                ? arm.RequestSerial
                : null;
        }
    }

    internal void ClearScannerRequest(Guid attemptId) {
        lock (_gate) {
            if (_currentScannerArm is { AttemptId: var armedAttemptId } && armedAttemptId == attemptId) {
                _currentScannerArm = null;
            }
        }
    }

    internal void CompleteScannerRequest(Guid attemptId, bool success, string reason) {
        ArgumentException.ThrowIfNullOrEmpty(reason);

        if (!ShouldClearScannerRequest(success, reason)) {
            return;
        }

        ClearScannerRequest(attemptId);
    }

    internal void ResetScannerRequest() {
        lock (_gate) {
            _currentScannerArm = null;
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
        return _openListingHook?.Original(agent, listingId) ?? false;
    }

    private bool OpenListingByContentIdDetour(nint agent, ulong contentId) {
        EnsureRequestCycleForIntercept(0U, contentId);
        return _openListingByContentIdHook?.Original(agent, contentId) ?? false;
    }

    private void EnsureRequestCycleForIntercept(uint listingId, ulong contentId) {
        lock (_gate) {
            if (_currentScannerArm is { } arm && IsCompatibleArm(arm, listingId, contentId)) {
                if (!arm.RequestSerial.HasValue) {
                    var cycle = _state.BeginRequest(
                        PartyDetailRequestOwner.Scanner,
                        arm.ListingId != 0 ? arm.ListingId : listingId,
                        arm.ContentId != 0 ? arm.ContentId : contentId
                    );

                    _currentScannerArm = arm with {
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

    private static bool IsCompatibleArm(ScannerArm arm, uint listingId, ulong contentId) {
        if (arm.ListingId != 0 && listingId != 0 && arm.ListingId != listingId) {
            return false;
        }

        if (arm.ContentId != 0 && contentId != 0 && arm.ContentId != contentId) {
            return false;
        }

        return true;
    }

    private static bool ShouldClearScannerRequest(bool success, string reason) {
        return success || reason.EndsWith("timeout", StringComparison.Ordinal);
    }

    private sealed record ScannerArm(Guid AttemptId, uint ListingId, ulong ContentId, long? RequestSerial);
}
