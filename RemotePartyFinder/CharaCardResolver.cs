using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Game.Addon.Lifecycle;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Misc;

#nullable enable

namespace RemotePartyFinder;

internal readonly record struct CharaCardPacketModel(
    ulong ContentId,
    ushort WorldId,
    string Name,
    uint SomeState = 0,
    byte Version = 1,
    byte Flags = 0,
    byte PrivacyFlags = 0
) {
    public bool IsNotCreated => Version == 0;
    public bool WasResetDueToFantasia => (Flags & 0x01) != 0;
    public bool IsVisibleToNoOne => (Flags & 0x02) != 0;
    public bool IsFriendsOnly => (PrivacyFlags & 0x01) != 0;
}

internal readonly record struct BannerHelperResponseModel(
    int ResponseCode,
    uint ResponseDetail
) {
    public bool IsKnownFailure =>
        ResponseCode != 0 || ResponseDetail is >= 1 and <= 4;
}

internal readonly record struct SelectOkDialogRequestModel(
    ulong ContentId,
    uint MessageId,
    int Variant,
    bool IsNotCreated,
    bool WasResetDueToFantasia
);

internal readonly record struct SelectOkStateTransitionModel(
    ulong ContentId,
    int Action,
    bool CanEdit,
    bool IsNotCreated,
    bool WasResetDueToFantasia
);

internal readonly record struct GameUiMessageModel(
    uint MessageId,
    uint Param = 0,
    bool HasParam = false
);

internal sealed class CharaCardResolver : IDisposable {
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultIdentityUploadAttemptTimeout = TimeSpan.FromSeconds(5);
    private const string IdentityUploadPath = "/contribute/character-identity";
    internal static readonly HashSet<uint> PlateFailureMessageIds = [
        0x16E0,
        0x16E8,
        0x16EA,
        0x16EB,
        0x16EC,
        0x16F6,
    ];
    internal static readonly AddonEvent[] SelectOkSuppressionEvents = [
        AddonEvent.PreSetup,
        AddonEvent.PreRequestedUpdate,
        AddonEvent.PreRefresh,
        AddonEvent.PreOpen,
        AddonEvent.PreShow,
    ];
    internal static readonly string[] SelectOkAddonNames = [
        "SelectOk",
        "SelectOkTitle",
    ];

    internal static int ResolveMaxAttemptsFromRetryCount(int configuredRetries) {
        return Math.Max(1, configuredRetries + 1);
    }

    private readonly PlayerLocalDatabase _database;
    private readonly ContentIdResolveQueue _queue;
    private readonly ICharaCardResolverRuntime _runtime;
    private readonly ISelectOkDialogSuppressionRuntime? _selectOkDialogSuppressionRuntime;
    private readonly PlateFailureSuppressionState _plateFailureSuppressionState = new();
    private readonly Func<ushort, string?> _worldNameResolver;
    private readonly Func<DateTime> _utcNow;
    private readonly Action<CharacterIdentitySnapshot> _persistResolvedIdentity;
    private readonly TimeSpan _requestTimeout;
    private readonly Action<string>? _debugSink;
    private readonly Action<string>? _warningSink;
    private readonly IdentityUploadPump? _identityUploadPump;
    private readonly HttpClient _identityUploadHttpClient;
    private readonly bool _ownsIdentityUploadHttpClient;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly Dictionary<ulong, PendingProjection> _pendingProjections = new();
    private readonly object _sync = new();
    private bool _disposed;

    internal CharaCardResolver(
        PlayerLocalDatabase database,
        ICharaCardResolverRuntime? runtime = null,
        Func<ushort, string?>? worldNameResolver = null,
        Func<DateTime>? utcNow = null,
        Action<string>? debugSink = null,
        Action<string>? warningSink = null,
        Action<CharacterIdentitySnapshot>? persistResolvedIdentity = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? identityUploadAttemptTimeout = null,
        Configuration? configuration = null,
        Func<IReadOnlyList<CharacterIdentityUploadPayload>, CancellationToken, Task<bool>>? uploadResolvedIdentitiesAsync = null,
        HttpClient? identityUploadHttpClient = null,
        ISelectOkDialogSuppressionRuntime? selectOkDialogSuppressionRuntime = null
    ) {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _debugSink = debugSink;
        _warningSink = warningSink;
        _runtime = runtime ?? new DalamudCharaCardResolverRuntime(warningSink);
        _selectOkDialogSuppressionRuntime = selectOkDialogSuppressionRuntime;
        _worldNameResolver = worldNameResolver ?? ResolveWorldName;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _persistResolvedIdentity = persistResolvedIdentity ?? _database.UpsertResolvedIdentity;
        _queue = new ContentIdResolveQueue(maxAttempts: ResolveMaxAttemptsFromRetryCount(configuration?.CharaCardResolveRetryCount ?? 1));
        _requestTimeout = requestTimeout ?? DefaultRequestTimeout;
        var resolvedIdentityUploadAttemptTimeout = identityUploadAttemptTimeout ?? DefaultIdentityUploadAttemptTimeout;
        _identityUploadHttpClient = identityUploadHttpClient ?? new HttpClient();
        _ownsIdentityUploadHttpClient = identityUploadHttpClient is null;
        var resolvedIdentityUploader = uploadResolvedIdentitiesAsync
            ?? (configuration is null
                ? null
                : (payloads, cancellationToken) => UploadResolvedIdentitiesAsync(
                    configuration,
                    payloads,
                    _identityUploadHttpClient,
                    resolvedIdentityUploadAttemptTimeout,
                    warningSink,
                    cancellationToken
                ));
        if (resolvedIdentityUploader is not null) {
            _identityUploadPump = new IdentityUploadPump(
                _database,
                _utcNow,
                resolvedIdentityUploader,
                _disposeCts.Token,
                debugSink,
                warningSink
            );
        }

        Preflight = _runtime.CheckAvailability();
        if (!Preflight.Enabled) {
            LogWarning($"CharaCardResolver: disabled during preflight. {Preflight.Reason}");
            return;
        }

        try {
            _runtime.Initialize(
                HandlePacket,
                HandleAgentPacket,
                HandleResponseDispatcher,
                HandleSelectOkStateTransition,
                HandleSimpleGameUiMessage,
                HandleParameterizedGameUiMessage,
                HandleSelectOkDialogRequest
            );
            TryInitializeSelectOkSuppressionRuntime();
            IsEnabled = true;
        } catch (Exception exception) {
            DisableEnrichment($"failed to initialize hook runtime: {exception.Message}");
        }
    }

    internal ResolverPreflightResult Preflight { get; }

    internal bool IsEnabled { get; private set; }

    internal void EnqueueMany(IEnumerable<ulong> contentIds) {
        ArgumentNullException.ThrowIfNull(contentIds);

        if (_disposed || !IsEnabled) {
            return;
        }

        var nowUtc = _utcNow();
        lock (_sync) {
            foreach (var contentId in contentIds) {
                if (contentId == 0 || _database.TryGetIdentity(contentId, out _)) {
                    continue;
                }

                _queue.Enqueue(contentId, nowUtc);
            }
        }
    }

    internal void OnFrameworkUpdate(IFramework framework) {
        Pump();
    }

    internal void Pump() {
        if (_disposed) {
            return;
        }

        _identityUploadPump?.PumpPendingIdentityUploads();

        if (!IsEnabled) {
            return;
        }

        var nowUtc = _utcNow();

        lock (_sync) {
            ExpireStaleInflightRequests(nowUtc);
        }

        if (TryPersistPendingProjection(nowUtc)) {
            return;
        }

        ResolveRequestStatus request;
        lock (_sync) {
            if (!_queue.TryStartNext(nowUtc, out request)) {
                return;
            }
        }

        try {
            if (!_runtime.TryRequest(request.ContentId)) {
                LogWarning($"CharaCardResolver: failed to dispatch identity request for contentId={request.ContentId}.");
                lock (_sync) {
                    _queue.MarkLocalFailure(request.ContentId, request.AttemptVersion, _utcNow());
                }

                return;
            }

            lock (_sync) {
                _plateFailureSuppressionState.ArmDispatchedRequestUiSuppression(_utcNow());
            }
            LogDebug($"CharaCardResolver: dispatched identity request for contentId={request.ContentId}.");
        } catch (Exception exception) {
            DisableEnrichment($"request dispatch failed: {exception.Message}");
        }
    }

    internal ResolveState GetResolveState(ulong contentId) {
        lock (_sync) {
            return _queue.GetState(contentId);
        }
    }

    internal async Task<bool> DrainPendingIdentityUploadsOnceAsync(CancellationToken cancellationToken) {
        if (_disposed || _identityUploadPump is null) {
            return false;
        }

        return await _identityUploadPump.DrainPendingIdentityUploadsOnceAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static ResolverPreflightResult CheckAvailability() {
        return new ResolverPreflightResult(true, "Hook signatures will be resolved during initialization.");
    }

    internal static bool TryProjectIdentity(
        CharaCardPacketModel packet,
        Func<ushort, string?> worldNameResolver,
        DateTime observedAtUtc,
        out CharacterIdentitySnapshot snapshot
    ) {
        ArgumentNullException.ThrowIfNull(worldNameResolver);

        snapshot = default!;
        if (packet.ContentId == 0 || packet.WorldId == 0 || string.IsNullOrWhiteSpace(packet.Name)) {
            return false;
        }

        var worldName = worldNameResolver(packet.WorldId);
        if (string.IsNullOrWhiteSpace(worldName)) {
            return false;
        }

        snapshot = new CharacterIdentitySnapshot(
            packet.ContentId,
            packet.Name.Trim(),
            packet.WorldId,
            worldName,
            NormalizeUtc(observedAtUtc)
        );
        return true;
    }

    public void Dispose() {
        if (_disposed) {
            return;
        }

        _disposed = true;
        IsEnabled = false;
        _disposeCts.Cancel();
        _selectOkDialogSuppressionRuntime?.Dispose();
        _runtime.Dispose();
        _disposeCts.Dispose();
        if (_ownsIdentityUploadHttpClient) {
            _identityUploadHttpClient.Dispose();
        }
    }

    private bool HandlePacket(CharaCardPacketModel packet) {
        if (_disposed || !IsEnabled) {
            return true;
        }

        ResolveRequestStatus request;
        var matchedByFallbackInflight = false;
        lock (_sync) {
            if (!_queue.TryGetRequest(packet.ContentId, out request)
                || request.State != ResolveState.InFlight) {
                if (!_queue.TryGetInFlightRequest(out request)) {
                    LogDebug(
                        $"CharaCardResolver: ignoring untracked packet contentId={packet.ContentId} " +
                        $"someState={packet.SomeState} version={packet.Version} flags=0x{packet.Flags:X2} privacy=0x{packet.PrivacyFlags:X2}."
                    );
                    return true;
                }

                matchedByFallbackInflight = true;
            }

            _plateFailureSuppressionState.TrackUiOpenBudget(request.ContentId);
        }

        if (packet.IsNotCreated || packet.WasResetDueToFantasia) {
            lock (_sync) {
                _plateFailureSuppressionState.ArmFailureSuppression(_utcNow());
                _plateFailureSuppressionState.TrackSelectOkDialogBudget(request.ContentId);
                if (_queue.MarkTimeout(request.ContentId, request.AttemptVersion, _utcNow())
                    && _queue.GetState(request.ContentId) == ResolveState.FailedPermanent) {
                    LogWarning($"CharaCardResolver: giving up on contentId={request.ContentId} after repeated plate-unavailable responses.");
                }
            }

            var reason = packet.IsNotCreated ? "not_created" : "fantasia_reset";
            LogDebug(
                $"CharaCardResolver: received plate-unavailable packet for packetContentId={packet.ContentId} " +
                $"trackedContentId={request.ContentId} reason={reason} fallback={matchedByFallbackInflight}, suppressing game UI propagation."
            );
            return false;
        }

        if (TryProjectIdentity(packet, _worldNameResolver, _utcNow(), out var snapshot)) {
            lock (_sync) {
                _pendingProjections[snapshot.ContentId] = new PendingProjection(snapshot, request.AttemptVersion);
            }
        } else {
            lock (_sync) {
                _plateFailureSuppressionState.ArmFailureSuppression(_utcNow());
                _plateFailureSuppressionState.TrackSelectOkDialogBudget(request.ContentId);
                if (_queue.MarkTimeout(request.ContentId, request.AttemptVersion, _utcNow())
                    && _queue.GetState(request.ContentId) == ResolveState.FailedPermanent) {
                    LogWarning($"CharaCardResolver: giving up on contentId={request.ContentId} after repeated incomplete plate responses.");
                }
            }
            LogDebug(
                $"CharaCardResolver: received incomplete plate response for packetContentId={packet.ContentId} " +
                $"trackedContentId={request.ContentId} fallback={matchedByFallbackInflight}, suppressing game UI propagation."
            );
        }

        return false;
    }

    private bool HandleAgentPacket(CharaCardPacketModel packet) {
        return HandleUiOpenPacket(packet.ContentId, "AgentCharaCard.OpenCharaCardForPacket");
    }

    private bool HandleSelectOkDialogRequest(SelectOkDialogRequestModel request) {
        if (_disposed || !IsEnabled) {
            return true;
        }

        lock (_sync) {
            if (!_plateFailureSuppressionState.TryConsumeSuppressedSelectOkDialog(request.ContentId)) {
                return true;
            }
        }

        LogDebug(
            $"CharaCardResolver: swallowed final SelectOk dialog request for contentId={request.ContentId} " +
            $"messageId=0x{request.MessageId:X} variant={request.Variant}."
        );
        return false;
    }

    private bool HandleSelectOkStateTransition(SelectOkStateTransitionModel request) {
        if (_disposed || !IsEnabled) {
            return true;
        }

        if (request.Action is >= 1 and <= 6) {
            return true;
        }

        lock (_sync) {
            if (!_plateFailureSuppressionState.TryConsumeSuppressedSelectOkDialog(request.ContentId)) {
                return true;
            }
        }

        LogDebug(
            $"CharaCardResolver: swallowed SelectOk state transition for contentId={request.ContentId} " +
            $"action={request.Action} canEdit={request.CanEdit} isNotCreated={request.IsNotCreated} " +
            $"wasResetDueToFantasia={request.WasResetDueToFantasia}."
        );
        return false;
    }

    private bool HandleSimpleGameUiMessage(GameUiMessageModel message) {
        if (!PlateFailureMessageIds.Contains(message.MessageId)) {
            return true;
        }

        return !ShouldSuppressGameUiText($"GameUiMessage.0x{message.MessageId:X}");
    }

    private bool HandleParameterizedGameUiMessage(GameUiMessageModel message) {
        if (!PlateFailureMessageIds.Contains(message.MessageId)) {
            return true;
        }

        return !ShouldSuppressGameUiText($"GameUiMessage.0x{message.MessageId:X}({message.Param})");
    }

    private bool HandleUiOpenPacket(ulong contentId, string source) {
        if (_disposed || !IsEnabled) {
            return true;
        }

        lock (_sync) {
            if (!_plateFailureSuppressionState.TryConsumeSuppressedUiOpen(contentId)) {
                return true;
            }
        }

        LogDebug($"CharaCardResolver: swallowed {source} for plugin-owned contentId={contentId}.");
        return false;
    }

    private bool HandleResponseDispatcher(BannerHelperResponseModel response) {
        if (_disposed || !IsEnabled || !response.IsKnownFailure) {
            return false;
        }

        ResolveRequestStatus request;
        lock (_sync) {
            if (!_queue.TryGetInFlightRequest(out request)) {
                return false;
            }

            _plateFailureSuppressionState.ArmFailureSuppression(_utcNow());
            _plateFailureSuppressionState.TrackSelectOkDialogBudget(request.ContentId);
            if (_queue.MarkTimeout(request.ContentId, request.AttemptVersion, _utcNow())
                && _queue.GetState(request.ContentId) == ResolveState.FailedPermanent) {
                LogWarning($"CharaCardResolver: giving up on contentId={request.ContentId} after repeated BannerHelper failure responses.");
            }
        }

        LogDebug($"CharaCardResolver: suppressed CharaCard update failure response for contentId={request.ContentId} code={response.ResponseCode} detail={response.ResponseDetail}.");
        return true;
    }

    private bool ShouldSuppressGameUiText(string source) {
        if (_disposed || !IsEnabled) {
            return false;
        }

        lock (_sync) {
            var hasInFlightRequest = _queue.TryGetInFlightRequest(out var request);
            var suppression = _plateFailureSuppressionState.GetGameUiSuppression(_utcNow(), hasInFlightRequest);
            if (suppression == SuppressionConsumption.WindowBudget) {
                LogDebug($"CharaCardResolver: suppressed {source} for plugin-owned plate request.");
                return true;
            }

            if (suppression == SuppressionConsumption.InFlightRequest && hasInFlightRequest) {
                LogDebug(
                    $"CharaCardResolver: suppressed {source} while contentId={request.ContentId} remains in-flight."
                );
                return true;
            }

            return false;
        }
    }

    private bool ShouldSuppressSelectOkAddon(string source) {
        if (_disposed || !IsEnabled) {
            return false;
        }

        lock (_sync) {
            var hasInFlightRequest = _queue.TryGetInFlightRequest(out var request);
            var suppression = _plateFailureSuppressionState.GetSelectOkSuppression(_utcNow(), hasInFlightRequest);
            if (suppression == SuppressionConsumption.WindowBudget) {
                LogDebug($"CharaCardResolver: suppressed {source} for plugin-owned plate request.");
                return true;
            }

            if (suppression == SuppressionConsumption.InFlightRequest && hasInFlightRequest) {
                LogDebug(
                    $"CharaCardResolver: suppressed {source} while contentId={request.ContentId} remains in-flight."
                );
                return true;
            }

            return false;
        }
    }

    private void DisableEnrichment(string reason) {
        IsEnabled = false;
        LogWarning($"CharaCardResolver: disabled. {reason}");

        try {
            _selectOkDialogSuppressionRuntime?.Dispose();
            _runtime.Dispose();
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to dispose runtime after disable. {exception.Message}");
        }
    }

    private void TryInitializeSelectOkSuppressionRuntime() {
        if (_selectOkDialogSuppressionRuntime is null) {
            return;
        }

        try {
            _selectOkDialogSuppressionRuntime.Initialize(ShouldSuppressSelectOkAddon);
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to initialize SelectOk suppression runtime. {exception.Message}");
        }
    }

    private static DateTime NormalizeUtc(DateTime value) {
        return value.Kind switch {
            DateTimeKind.Utc => value,
            DateTimeKind.Unspecified => DateTime.SpecifyKind(value, DateTimeKind.Utc),
            _ => value.ToUniversalTime(),
        };
    }

    private static unsafe string? ResolveWorldName(ushort worldId) {
        var worldHelper = WorldHelper.Instance();
        if (worldHelper == null) {
            return null;
        }

        return worldHelper->GetWorldNameById(worldId);
    }

    private void LogWarning(string message) {
        _warningSink?.Invoke(message);
    }

    private void LogDebug(string message) {
        _debugSink?.Invoke(message);
    }

    private void ExpireStaleInflightRequests(DateTime nowUtc) {
        foreach (var request in _queue.Requests) {
            if (request.State != ResolveState.InFlight) {
                continue;
            }

            if (_pendingProjections.ContainsKey(request.ContentId)) {
                continue;
            }

            if (request.LastRequestedAtUtc == DateTime.MinValue) {
                continue;
            }

            if (nowUtc - request.LastRequestedAtUtc < _requestTimeout) {
                continue;
            }

            if (_queue.MarkTimeout(request.ContentId, request.AttemptVersion, nowUtc)
                && _queue.GetState(request.ContentId) == ResolveState.FailedPermanent) {
                LogWarning($"CharaCardResolver: giving up on contentId={request.ContentId} after repeated plate request failures.");
            }
        }
    }

    private bool TryPersistPendingProjection(DateTime nowUtc) {
        PendingProjection? candidate = null;

        lock (_sync) {
            foreach (var entry in _pendingProjections.OrderBy(static entry => entry.Key)) {
                candidate = entry.Value;
                break;
            }
        }

        if (candidate is null) {
            return false;
        }

        try {
            _persistResolvedIdentity(candidate.Snapshot);
            lock (_sync) {
                _pendingProjections.Remove(candidate.Snapshot.ContentId);
                _queue.MarkResolved(candidate.Snapshot);
            }

            LogDebug(
                $"CharaCardResolver: persisted resolved identity contentId={candidate.Snapshot.ContentId} " +
                $"name=\"{candidate.Snapshot.Name}\" homeWorld={candidate.Snapshot.HomeWorld} " +
                $"worldName=\"{candidate.Snapshot.WorldName}\"."
            );
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to persist resolved identity for {candidate.Snapshot.ContentId}. {exception.Message}");
            lock (_sync) {
                _pendingProjections.Remove(candidate.Snapshot.ContentId);
                _queue.MarkLocalFailure(candidate.Snapshot.ContentId, candidate.AttemptVersion, nowUtc);
            }
        }

        return true;
    }

    private static async Task<bool> UploadResolvedIdentitiesAsync(
        Configuration configuration,
        IReadOnlyList<CharacterIdentityUploadPayload> payloads,
        HttpClient httpClient,
        TimeSpan attemptTimeout,
        Action<string>? warningSink,
        CancellationToken cancellationToken
    ) {
        if (payloads.Count == 0) {
            return false;
        }

        var jsonPayload = JsonSerializer.Serialize(payloads);
        var anySuccess = false;

        foreach (var uploadTarget in configuration.UploadUrls.Where(static candidate => candidate.IsEnabled)) {
            if (IsCircuitOpen(configuration, uploadTarget)) {
                continue;
            }

            if (!IngestEndpointResolver.TryBuildEndpointUrl(uploadTarget, IdentityUploadPath, out var endpointUrl)) {
                continue;
            }

            try {
                using var request = IngestRequestFactory.CreatePostJsonRequest(
                    configuration,
                    endpointUrl,
                    IdentityUploadPath,
                    jsonPayload
                );
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(attemptTimeout);
                using var response = await httpClient.SendAsync(request, attemptCts.Token).ConfigureAwait(false);

                if (response.IsSuccessStatusCode) {
                    anySuccess = true;
                    uploadTarget.FailureCount = 0;
                    continue;
                }

                uploadTarget.FailureCount++;
                uploadTarget.LastFailureTime = DateTime.UtcNow;

                if ((int)response.StatusCode == 429) {
                    var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(response);
                    if (retryAfter.HasValue) {
                        warningSink?.Invoke($"CharaCardResolver: rate limited by {endpointUrl}, retry_after={retryAfter.Value}s");
                    }
                }
            } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                throw;
            } catch (OperationCanceledException) {
                uploadTarget.FailureCount++;
                uploadTarget.LastFailureTime = DateTime.UtcNow;
                warningSink?.Invoke($"CharaCardResolver: identity upload timed out for {endpointUrl} after {attemptTimeout.TotalMilliseconds:0}ms.");
            } catch (Exception exception) {
                uploadTarget.FailureCount++;
                uploadTarget.LastFailureTime = DateTime.UtcNow;
                warningSink?.Invoke($"CharaCardResolver: identity upload error to {endpointUrl}. {exception.Message}");
            }
        }

        if (!anySuccess) {
            warningSink?.Invoke($"CharaCardResolver: identity upload failed, {payloads.Count} identities remain pending.");
        }

        return anySuccess;
    }

    private static bool IsCircuitOpen(Configuration configuration, UploadUrl uploadTarget) {
        if (uploadTarget.FailureCount < configuration.CircuitBreakerFailureThreshold) {
            return false;
        }

        var elapsedSinceFailure = DateTime.UtcNow - uploadTarget.LastFailureTime;
        return elapsedSinceFailure.TotalMinutes < configuration.CircuitBreakerBreakDurationMinutes;
    }

    private sealed record PendingProjection(
        CharacterIdentitySnapshot Snapshot,
        int AttemptVersion
    );
}
