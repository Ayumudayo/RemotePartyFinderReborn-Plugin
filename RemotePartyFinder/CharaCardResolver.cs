using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Game.Chat;
using Dalamud.Game.Addon.Lifecycle;
using Dalamud.Game.Addon.Lifecycle.AddonArgTypes;
using Dalamud.Game.Gui.Toast;
using Dalamud.Hooking;
using Dalamud.Plugin.Services;
using ECommons.DalamudServices;
using FFXIVClientStructs.FFXIV.Client.Game.UI;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;
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

internal interface ICharaCardResolverRuntime : IDisposable {
    ResolverPreflightResult CheckAvailability();
    void Initialize(
        Func<CharaCardPacketModel, bool> packetHandler,
        Func<CharaCardPacketModel, bool> agentPacketHandler,
        Func<BannerHelperResponseModel, bool> bannerHelperResponseHandler,
        Func<CharaCardPacketModel, bool> bannerHelperPacketHandler,
        Func<SelectOkStateTransitionModel, bool> selectOkStateTransitionHandler,
        Func<GameUiMessageModel, bool> simpleGameUiMessageHandler,
        Func<GameUiMessageModel, bool> parameterizedGameUiMessageHandler,
        Func<SelectOkDialogRequestModel, bool> selectOkDialogHandler
    );
    bool TryRequest(ulong contentId);
}

internal interface ISelectOkDialogSuppressionRuntime : IDisposable {
    void Initialize(Func<string, bool> selectOkHandler);
}

internal sealed class CharaCardResolver : IDisposable {
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultIdentityUploadAttemptTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan GameLogSuppressionWindow = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan SelectOkSuppressionWindow = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan[] IdentityUploadRetryBackoffSchedule = [
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(1),
    ];
    private const int GameLogSuppressionBudget = 32;
    private const int SelectOkSuppressionBudget = 8;
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

    private readonly PlayerLocalDatabase _database;
    private readonly ContentIdResolveQueue _queue = new();
    private readonly ICharaCardResolverRuntime _runtime;
    private readonly ISelectOkDialogSuppressionRuntime? _selectOkDialogSuppressionRuntime;
    private readonly Func<ushort, string?> _worldNameResolver;
    private readonly Func<DateTime> _utcNow;
    private readonly Action<CharacterIdentitySnapshot> _persistResolvedIdentity;
    private readonly TimeSpan _requestTimeout;
    private readonly TimeSpan _identityUploadAttemptTimeout;
    private readonly Action<string>? _debugSink;
    private readonly Action<string>? _warningSink;
    private readonly Func<IReadOnlyList<CharacterIdentityUploadPayload>, CancellationToken, Task<bool>>? _uploadResolvedIdentitiesAsync;
    private readonly HttpClient _identityUploadHttpClient;
    private readonly bool _ownsIdentityUploadHttpClient;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly Dictionary<ulong, PendingProjection> _pendingProjections = new();
    private readonly Dictionary<ulong, int> _suppressedUiOpenBudgets = [];
    private readonly Dictionary<ulong, int> _suppressedSelectOkDialogBudgets = [];
    private readonly object _sync = new();
    private int _identityUploadWorkerBusy;
    private int _identityUploadFailureCount;
    private DateTime _nextIdentityUploadAttemptAtUtc = DateTime.MinValue;
    private int _suppressedGameLogMessagesRemaining;
    private DateTime _suppressGameLogMessagesUntilUtc = DateTime.MinValue;
    private int _suppressedSelectOkEventsRemaining;
    private DateTime _suppressSelectOkUntilUtc = DateTime.MinValue;
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
        _requestTimeout = requestTimeout ?? DefaultRequestTimeout;
        _identityUploadAttemptTimeout = identityUploadAttemptTimeout ?? DefaultIdentityUploadAttemptTimeout;
        _identityUploadHttpClient = identityUploadHttpClient ?? new HttpClient();
        _ownsIdentityUploadHttpClient = identityUploadHttpClient is null;
        _uploadResolvedIdentitiesAsync = uploadResolvedIdentitiesAsync
            ?? (configuration is null
                ? null
                : (payloads, cancellationToken) => UploadResolvedIdentitiesAsync(
                    configuration,
                    payloads,
                    _identityUploadHttpClient,
                    _identityUploadAttemptTimeout,
                    warningSink,
                    cancellationToken
                ));

        Preflight = _runtime.CheckAvailability();
        if (!Preflight.Enabled) {
            LogWarning($"CharaCardResolver: disabled during preflight. {Preflight.Reason}");
            return;
        }

        try {
            _runtime.Initialize(
                HandlePacket,
                HandleAgentPacket,
                HandleBannerHelperResponse,
                HandleBannerHelperPacket,
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

        PumpPendingIdentityUploads();

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

            ArmDispatchedRequestUiSuppression(_utcNow());
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
        if (_disposed || _uploadResolvedIdentitiesAsync is null) {
            return false;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var snapshots = _database.TakePendingIdentityUploads(PlayerLocalDatabase.MaxBatchSize);
        if (snapshots.Count == 0) {
            return false;
        }

        var payloads = snapshots
            .Select(static snapshot => CharacterIdentityUploadPayload.FromSnapshot(snapshot, snapshot.LastResolvedAtUtc))
            .ToArray();

        bool uploadSucceeded;
        try {
            uploadSucceeded = await _uploadResolvedIdentitiesAsync(payloads, cancellationToken).ConfigureAwait(false);
        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || _disposeCts.IsCancellationRequested) {
            return false;
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to upload resolved identities. {exception.Message}");
            return false;
        }

        if (!uploadSucceeded) {
            return false;
        }

        try {
            _database.MarkIdentityUploadsSubmitted(payloads.Select(static payload => payload.ContentId));
            LogDebug($"CharaCardResolver: uploaded resolved identity batch count={payloads.Count()}.");
            return true;
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to mark identity uploads as submitted. {exception.Message}");
            return false;
        }
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

            _suppressedUiOpenBudgets[request.ContentId] = 2;
        }

        if (packet.IsNotCreated || packet.WasResetDueToFantasia) {
            ArmFailureUiSuppression(_utcNow());
            lock (_sync) {
                _suppressedSelectOkDialogBudgets[request.ContentId] = 2;
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
            ArmFailureUiSuppression(_utcNow());
            lock (_sync) {
                _suppressedSelectOkDialogBudgets[request.ContentId] = 2;
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

    private bool HandleBannerHelperPacket(CharaCardPacketModel packet) {
        return HandleUiOpenPacket(packet.ContentId, "BannerHelper.OpenCharaCardForPacket");
    }

    private bool HandleSelectOkDialogRequest(SelectOkDialogRequestModel request) {
        if (_disposed || !IsEnabled) {
            return true;
        }

        lock (_sync) {
            if (!TryConsumeSuppressedSelectOkDialog(request.ContentId)) {
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
            if (!TryConsumeSuppressedSelectOkDialog(request.ContentId)) {
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
            if (!TryConsumeSuppressedUiOpen(contentId)) {
                return true;
            }
        }

        LogDebug($"CharaCardResolver: swallowed {source} for plugin-owned contentId={contentId}.");
        return false;
    }

    private bool HandleBannerHelperResponse(BannerHelperResponseModel response) {
        if (_disposed || !IsEnabled || !response.IsKnownFailure) {
            return false;
        }

        ResolveRequestStatus request;
        lock (_sync) {
            if (!_queue.TryGetInFlightRequest(out request)) {
                return false;
            }

            ArmFailureUiSuppression(_utcNow());
            _suppressedSelectOkDialogBudgets[request.ContentId] = 2;
            if (_queue.MarkTimeout(request.ContentId, request.AttemptVersion, _utcNow())
                && _queue.GetState(request.ContentId) == ResolveState.FailedPermanent) {
                LogWarning($"CharaCardResolver: giving up on contentId={request.ContentId} after repeated BannerHelper failure responses.");
            }
        }

        LogDebug($"CharaCardResolver: suppressed BannerHelper failure response for contentId={request.ContentId} code={response.ResponseCode} detail={response.ResponseDetail}.");
        return true;
    }

    private void ArmDispatchedRequestUiSuppression(DateTime observedAtUtc) {
        lock (_sync) {
            _suppressedGameLogMessagesRemaining = GameLogSuppressionBudget;
            _suppressGameLogMessagesUntilUtc = observedAtUtc.Add(GameLogSuppressionWindow);
        }
    }

    private void ArmFailureUiSuppression(DateTime observedAtUtc) {
        ArmDispatchedRequestUiSuppression(observedAtUtc);
        lock (_sync) {
            _suppressedSelectOkEventsRemaining = SelectOkSuppressionBudget;
            _suppressSelectOkUntilUtc = observedAtUtc.Add(SelectOkSuppressionWindow);
        }
    }

    private bool TryConsumeSuppressedUiOpen(ulong contentId) {
        if (!_suppressedUiOpenBudgets.TryGetValue(contentId, out var remaining) || remaining <= 0) {
            return false;
        }

        if (remaining == 1) {
            _suppressedUiOpenBudgets.Remove(contentId);
            return true;
        }

        _suppressedUiOpenBudgets[contentId] = remaining - 1;
        return true;
    }

    private bool TryConsumeSuppressedSelectOkDialog(ulong contentId) {
        if (!_suppressedSelectOkDialogBudgets.TryGetValue(contentId, out var remaining) || remaining <= 0) {
            return false;
        }

        if (remaining == 1) {
            _suppressedSelectOkDialogBudgets.Remove(contentId);
            return true;
        }

        _suppressedSelectOkDialogBudgets[contentId] = remaining - 1;
        return true;
    }

    private bool ShouldSuppressGameUiText(string source) {
        if (_disposed || !IsEnabled) {
            return false;
        }

        lock (_sync) {
            var nowUtc = _utcNow();
            if (nowUtc > _suppressGameLogMessagesUntilUtc) {
                _suppressedGameLogMessagesRemaining = 0;
            }

            if (_suppressedGameLogMessagesRemaining > 0) {
                _suppressedGameLogMessagesRemaining--;
                LogDebug($"CharaCardResolver: suppressed {source} for plugin-owned plate request.");
                return true;
            }

            if (_queue.TryGetInFlightRequest(out var request)) {
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
            var nowUtc = _utcNow();
            if (nowUtc > _suppressSelectOkUntilUtc) {
                _suppressedSelectOkEventsRemaining = 0;
            }

            if (_suppressedSelectOkEventsRemaining > 0) {
                _suppressedSelectOkEventsRemaining--;
                LogDebug($"CharaCardResolver: suppressed {source} for plugin-owned plate request.");
                return true;
            }

            if (_queue.TryGetInFlightRequest(out var request)) {
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

    private void PumpPendingIdentityUploads() {
        if (_uploadResolvedIdentitiesAsync is null) {
            return;
        }

        lock (_sync) {
            if (_utcNow() < _nextIdentityUploadAttemptAtUtc) {
                return;
            }
        }

        if (Interlocked.CompareExchange(ref _identityUploadWorkerBusy, 1, 0) != 0) {
            return;
        }

        if (_database.TakePendingIdentityUploads(1).Count == 0) {
            Interlocked.Exchange(ref _identityUploadWorkerBusy, 0);
            return;
        }

        var cancellationToken = _disposeCts.Token;
        _ = Task.Run(async () => {
            var uploadSucceeded = false;
            try {
                uploadSucceeded = await DrainPendingIdentityUploadsOnceAsync(cancellationToken).ConfigureAwait(false);
            } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            } catch (Exception exception) {
                LogWarning($"CharaCardResolver: identity upload worker failed. {exception.Message}");
            } finally {
                if (!cancellationToken.IsCancellationRequested) {
                    RecordIdentityUploadAttempt(uploadSucceeded, _utcNow());
                }

                Interlocked.Exchange(ref _identityUploadWorkerBusy, 0);
            }
        }, cancellationToken);
    }

    private void RecordIdentityUploadAttempt(bool uploadSucceeded, DateTime observedAtUtc) {
        lock (_sync) {
            if (uploadSucceeded) {
                _identityUploadFailureCount = 0;
                _nextIdentityUploadAttemptAtUtc = DateTime.MinValue;
                return;
            }

            var nextDelay = IdentityUploadRetryBackoffSchedule[Math.Min(_identityUploadFailureCount, IdentityUploadRetryBackoffSchedule.Length - 1)];
            _identityUploadFailureCount++;
            _nextIdentityUploadAttemptAtUtc = observedAtUtc.Add(nextDelay);
        }
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

internal sealed class DalamudSelectOkDialogSuppressionRuntime : ISelectOkDialogSuppressionRuntime {
    private readonly IAddonLifecycle _addonLifecycle;
    private readonly IChatGui _chatGui;
    private readonly IToastGui _toastGui;
    private readonly Action<string>? _warningSink;
    private readonly IAddonLifecycle.AddonEventDelegate _handler;
    private readonly IChatGui.OnLogMessageDelegate _logMessageHandler;
    private readonly IToastGui.OnNormalToastDelegate _normalToastHandler;
    private readonly IToastGui.OnQuestToastDelegate _questToastHandler;
    private readonly IToastGui.OnErrorToastDelegate _errorToastHandler;
    private Func<string, bool>? _selectOkHandler;
    private bool _disposed;

    internal DalamudSelectOkDialogSuppressionRuntime(
        IAddonLifecycle addonLifecycle,
        IChatGui chatGui,
        IToastGui toastGui,
        Action<string>? warningSink = null
    ) {
        _addonLifecycle = addonLifecycle ?? throw new ArgumentNullException(nameof(addonLifecycle));
        _chatGui = chatGui ?? throw new ArgumentNullException(nameof(chatGui));
        _toastGui = toastGui ?? throw new ArgumentNullException(nameof(toastGui));
        _warningSink = warningSink;
        _handler = OnAddonEvent;
        _logMessageHandler = OnLogMessage;
        _normalToastHandler = OnNormalToast;
        _questToastHandler = OnQuestToast;
        _errorToastHandler = OnErrorToast;
    }

    public void Initialize(Func<string, bool> selectOkHandler) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _selectOkHandler = selectOkHandler ?? throw new ArgumentNullException(nameof(selectOkHandler));

        foreach (var addonName in CharaCardResolver.SelectOkAddonNames) {
            foreach (var eventType in CharaCardResolver.SelectOkSuppressionEvents) {
                _addonLifecycle.RegisterListener(eventType, addonName, _handler);
            }
        }

        _chatGui.LogMessage += _logMessageHandler;
        _toastGui.Toast += _normalToastHandler;
        _toastGui.QuestToast += _questToastHandler;
        _toastGui.ErrorToast += _errorToastHandler;
    }

    public void Dispose() {
        if (_disposed) {
            return;
        }

        _disposed = true;
        foreach (var addonName in CharaCardResolver.SelectOkAddonNames) {
            foreach (var eventType in CharaCardResolver.SelectOkSuppressionEvents) {
                _addonLifecycle.UnregisterListener(eventType, addonName, _handler);
            }
        }

        _chatGui.LogMessage -= _logMessageHandler;
        _toastGui.Toast -= _normalToastHandler;
        _toastGui.QuestToast -= _questToastHandler;
        _toastGui.ErrorToast -= _errorToastHandler;
    }

    private void OnAddonEvent(AddonEvent eventType, AddonArgs args) {
        try {
            if (_selectOkHandler?.Invoke($"{args.AddonName}.{eventType}") == true) {
                args.PreventOriginal();
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process SelectOk addon lifecycle event. {exception.Message}");
        }
    }

    private void OnLogMessage(ILogMessage message) {
        try {
            if (_selectOkHandler?.Invoke($"ChatLog.{message.LogMessageId}") == true) {
                message.PreventOriginal();
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process chat log suppression event. {exception.Message}");
        }
    }

    private void OnNormalToast(ref Dalamud.Game.Text.SeStringHandling.SeString message, ref ToastOptions options, ref bool isHandled) {
        try {
            if (_selectOkHandler?.Invoke("Toast.Normal") == true) {
                isHandled = true;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process normal toast suppression event. {exception.Message}");
        }
    }

    private void OnQuestToast(ref Dalamud.Game.Text.SeStringHandling.SeString message, ref QuestToastOptions options, ref bool isHandled) {
        try {
            if (_selectOkHandler?.Invoke("Toast.Quest") == true) {
                isHandled = true;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process quest toast suppression event. {exception.Message}");
        }
    }

    private void OnErrorToast(ref Dalamud.Game.Text.SeStringHandling.SeString message, ref bool isHandled) {
        try {
            if (_selectOkHandler?.Invoke("Toast.Error") == true) {
                isHandled = true;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process error toast suppression event. {exception.Message}");
        }
    }
}

internal unsafe sealed class DalamudCharaCardResolverRuntime : ICharaCardResolverRuntime {
    // These helpers are not exposed as generated interop yet, so resolve them by signature.
    private const string HandleCurrentCharaCardDataPacketSignature =
        "40 53 48 83 EC 20 8B 05 ?? ?? ?? ?? 48 8B DA 39 42 18 0F 85 48 05 00 00 83 7A 1C 03 0F 84 3E 05 00 00 0F B6 42 28 88 81 24 01 00 00 0F B6 42 29";
    private const string CharaCardUpdateResponseDispatcherSignature =
        "48 89 5C 24 08 57 48 83 EC 20 41 8B F8 8B DA 85 D2 0F 85 84 01 00 00 38 51 68 0F 84 3B 02 00 00 44 0F B6 81 E0 01 00 00 45 33 D2 41 F6 C0 04";
    private const string AgentOpenCharaCardForPacketSignature =
        "40 55 53 57 41 57 48 8D AC 24 48 FC FF FF 48 81 EC B8 04 00 00 48 8B 05 ?? ?? ?? ?? 48 33 C4 48 89 85 A0 03 00 00 48 83 79 28 00 45 0F B6 F8 48";
    private const string BannerHelperLogCharaCardUpdateResponseSignature =
        "40 53 56 57 48 83 EC 40 48 8B 05 ?? ?? ?? ?? 48 33 C4 48 89 44 24 38 41 8B D8 8B FA 48 8B F1 85 D2 74 1C 48 8B 49 08 48 8B 01 FF 50 58 44 8B C7";
    private const string BannerHelperOpenCharaCardForPacketSignature =
        "40 53 48 83 EC 20 48 8B 49 08 48 8B DA 48 8B 01 FF 90 28 01 00 00 48 8B C8 E8 ?? ?? ?? ?? 45 33 C0 48 8B D3 48 8B C8 48 83 C4 20 5B E9 ?? ?? ?? ??";
    private const string SelectOkStateTransitionSignature =
        "40 55 53 56 57 48 8D 6C 24 88 48 81 EC 78 01 00 00 48 8B 05 50 C2 A7 01 48 33 C4 48 89 45 60 8B 85 C0 00 00 00 49 8B F8 48 8B F2 48 8B D9 83 F8 01 75 17";
    private const string GameUiMessageSignature =
        "48 89 5C 24 10 48 89 74 24 18 48 89 7C 24 20 55 48 8D AC 24 80 FE FF FF 48 81 EC 80 02 00 00 48 8B 05 22 F9 E4 01 48 33 C4 48 89 85 70 01 00 00";
    private const string ParameterizedGameUiMessageSignature =
        "48 89 5C 24 10 48 89 74 24 18 55 57 41 56 48 8D AC 24 20 FE FF FF 48 81 EC E0 02 00 00 48 8B 05 C4 F6 E4 01 48 33 C4 48 89 85 D0 01 00 00 45 33 F6";
    private const string CreateSelectOkDialogSignature =
        "40 53 55 56 57 41 56 48 81 EC 90 00 00 00 48 8B 05 23 86 A7 01 48 33 C4 48 89 84 24 80 00 00 00 BF 04 00 00 00 41 8B E8 44 8B CF 48 8D 44 24 40";

    private readonly IGameInteropProvider _interopProvider;
    private readonly delegate*<CharaCard*> _instanceResolver;
    private readonly Action<string>? _warningSink;
    private Hook<HandleCurrentCharaCardDataPacketDelegate>? _packetHook;
    private Hook<CharaCardUpdateResponseDispatcherDelegate>? _responseStatusDispatcherHook;
    private Hook<AgentOpenCharaCardForPacketDelegate>? _agentPacketHook;
    private Hook<BannerHelperLogCharaCardUpdateResponseDelegate>? _bannerHelperLogCharaCardUpdateResponseHook;
    private Hook<BannerHelperOpenCharaCardForPacketDelegate>? _bannerHelperOpenCharaCardForPacketHook;
    private Hook<SelectOkStateTransitionDelegate>? _selectOkStateTransitionHook;
    private Hook<GameUiMessageDelegate>? _gameUiMessageHook;
    private Hook<ParameterizedGameUiMessageDelegate>? _parameterizedGameUiMessageHook;
    private Hook<CreateSelectOkDialogDelegate>? _createSelectOkDialogHook;
    private bool _loggedPacketDetourHit;
    private bool _loggedResponseStatusDetourHit;
    private bool _loggedSelectOkStateTransitionDetourHit;
    private bool _loggedSimpleGameUiMessageDetourHit;
    private bool _loggedParameterizedGameUiMessageDetourHit;
    private bool _loggedFinalSelectOkDialogDetourHit;
    private Func<CharaCardPacketModel, bool>? _packetHandler;
    private Func<CharaCardPacketModel, bool>? _agentPacketHandler;
    private Func<BannerHelperResponseModel, bool>? _bannerHelperResponseHandler;
    private Func<CharaCardPacketModel, bool>? _bannerHelperPacketHandler;
    private Func<SelectOkStateTransitionModel, bool>? _selectOkStateTransitionHandler;
    private Func<GameUiMessageModel, bool>? _simpleGameUiMessageHandler;
    private Func<GameUiMessageModel, bool>? _parameterizedGameUiMessageHandler;
    private Func<SelectOkDialogRequestModel, bool>? _selectOkDialogHandler;

    private delegate void HandleCurrentCharaCardDataPacketDelegate(CharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet);
    private delegate void CharaCardUpdateResponseDispatcherDelegate(nint thisPtr, int responseCode, uint responseDetail);
    private delegate void AgentOpenCharaCardForPacketDelegate(AgentCharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet, bool a3);
    private delegate void BannerHelperLogCharaCardUpdateResponseDelegate(nint thisPtr, int responseCode, uint responseDetail);
    private delegate void BannerHelperOpenCharaCardForPacketDelegate(nint thisPtr, AgentCharaCard.CharaCardPacket* packet);
    private delegate nint SelectOkStateTransitionDelegate(nint thisPtr, nint statePtr, nint eventPtr, nint arg4, int action);
    private delegate void GameUiMessageDelegate(nint thisPtr, uint messageId);
    private delegate void ParameterizedGameUiMessageDelegate(nint thisPtr, uint messageId, uint param);
    private delegate uint CreateSelectOkDialogDelegate(nint thisPtr, uint messageId, nuint variant);

    internal DalamudCharaCardResolverRuntime(Action<string>? warningSink = null)
        : this(Svc.Hook, &CharaCard.Instance, warningSink) {
    }

    internal DalamudCharaCardResolverRuntime(
        IGameInteropProvider interopProvider,
        delegate*<CharaCard*> instanceResolver,
        Action<string>? warningSink = null
    ) {
        _interopProvider = interopProvider ?? throw new ArgumentNullException(nameof(interopProvider));
        _instanceResolver = instanceResolver;
        _warningSink = warningSink;
    }

    public ResolverPreflightResult CheckAvailability() => CharaCardResolver.CheckAvailability();

    public void Initialize(
        Func<CharaCardPacketModel, bool> packetHandler,
        Func<CharaCardPacketModel, bool> agentPacketHandler,
        Func<BannerHelperResponseModel, bool> bannerHelperResponseHandler,
        Func<CharaCardPacketModel, bool> bannerHelperPacketHandler,
        Func<SelectOkStateTransitionModel, bool> selectOkStateTransitionHandler,
        Func<GameUiMessageModel, bool> simpleGameUiMessageHandler,
        Func<GameUiMessageModel, bool> parameterizedGameUiMessageHandler,
        Func<SelectOkDialogRequestModel, bool> selectOkDialogHandler
    ) {
        ArgumentNullException.ThrowIfNull(packetHandler);
        ArgumentNullException.ThrowIfNull(agentPacketHandler);
        ArgumentNullException.ThrowIfNull(bannerHelperResponseHandler);
        ArgumentNullException.ThrowIfNull(bannerHelperPacketHandler);
        ArgumentNullException.ThrowIfNull(selectOkStateTransitionHandler);
        ArgumentNullException.ThrowIfNull(simpleGameUiMessageHandler);
        ArgumentNullException.ThrowIfNull(parameterizedGameUiMessageHandler);
        ArgumentNullException.ThrowIfNull(selectOkDialogHandler);

        _packetHandler = packetHandler;
        _agentPacketHandler = agentPacketHandler;
        _bannerHelperResponseHandler = bannerHelperResponseHandler;
        _bannerHelperPacketHandler = bannerHelperPacketHandler;
        _selectOkStateTransitionHandler = selectOkStateTransitionHandler;
        _simpleGameUiMessageHandler = simpleGameUiMessageHandler;
        _parameterizedGameUiMessageHandler = parameterizedGameUiMessageHandler;
        _selectOkDialogHandler = selectOkDialogHandler;
        _packetHook = _interopProvider.HookFromSignature<HandleCurrentCharaCardDataPacketDelegate>(
            HandleCurrentCharaCardDataPacketSignature,
            HandleCurrentCharaCardDataPacketDetour
        );
        _responseStatusDispatcherHook = _interopProvider.HookFromSignature<CharaCardUpdateResponseDispatcherDelegate>(
            CharaCardUpdateResponseDispatcherSignature,
            CharaCardUpdateResponseDispatcherDetour
        );
        _agentPacketHook = _interopProvider.HookFromSignature<AgentOpenCharaCardForPacketDelegate>(
            AgentOpenCharaCardForPacketSignature,
            AgentOpenCharaCardForPacketDetour
        );
        TryInitializeOptionalSelectOkStateTransitionHook();
        TryInitializeOptionalGameUiMessageHooks();
        _createSelectOkDialogHook = _interopProvider.HookFromSignature<CreateSelectOkDialogDelegate>(
            CreateSelectOkDialogSignature,
            CreateSelectOkDialogDetour
        );
        TryInitializeOptionalBannerHelperHooks();
        _packetHook.Enable();
        _responseStatusDispatcherHook.Enable();
        _agentPacketHook.Enable();
        _selectOkStateTransitionHook?.Enable();
        _gameUiMessageHook?.Enable();
        _parameterizedGameUiMessageHook?.Enable();
        _createSelectOkDialogHook.Enable();
        _bannerHelperLogCharaCardUpdateResponseHook?.Enable();
        _bannerHelperOpenCharaCardForPacketHook?.Enable();
        _warningSink?.Invoke(
            "CharaCardResolver: hook init " +
            $"packet=true responseDispatcher=true agent=true stateTransition={_selectOkStateTransitionHook is not null} " +
            $"simpleMessage={_gameUiMessageHook is not null} parameterizedMessage={_parameterizedGameUiMessageHook is not null} " +
            $"finalSelectOk=true bannerLog={_bannerHelperLogCharaCardUpdateResponseHook is not null} " +
            $"bannerPacket={_bannerHelperOpenCharaCardForPacketHook is not null}"
        );
    }

    public bool TryRequest(ulong contentId) {
        var charaCard = _instanceResolver();
        if (charaCard == null) {
            return false;
        }

        charaCard->RequestCharaCardForContentId(contentId);
        return true;
    }

    public void Dispose() {
        _packetHandler = null;
        _agentPacketHandler = null;
        _bannerHelperResponseHandler = null;
        _bannerHelperPacketHandler = null;
        _selectOkStateTransitionHandler = null;
        _simpleGameUiMessageHandler = null;
        _parameterizedGameUiMessageHandler = null;
        _selectOkDialogHandler = null;
        _packetHook?.Dispose();
        _packetHook = null;
        _responseStatusDispatcherHook?.Dispose();
        _responseStatusDispatcherHook = null;
        _agentPacketHook?.Dispose();
        _agentPacketHook = null;
        _selectOkStateTransitionHook?.Dispose();
        _selectOkStateTransitionHook = null;
        _gameUiMessageHook?.Dispose();
        _gameUiMessageHook = null;
        _parameterizedGameUiMessageHook?.Dispose();
        _parameterizedGameUiMessageHook = null;
        _createSelectOkDialogHook?.Dispose();
        _createSelectOkDialogHook = null;
        _bannerHelperLogCharaCardUpdateResponseHook?.Dispose();
        _bannerHelperLogCharaCardUpdateResponseHook = null;
        _bannerHelperOpenCharaCardForPacketHook?.Dispose();
        _bannerHelperOpenCharaCardForPacketHook = null;
    }

    private void TryInitializeOptionalSelectOkStateTransitionHook() {
        try {
            _selectOkStateTransitionHook = _interopProvider.HookFromSignature<SelectOkStateTransitionDelegate>(
                SelectOkStateTransitionSignature,
                SelectOkStateTransitionDetour
            );
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to initialize SelectOk state transition hook. {exception.Message}");
            _selectOkStateTransitionHook = null;
        }
    }

    private void TryInitializeOptionalGameUiMessageHooks() {
        try {
            _gameUiMessageHook = _interopProvider.HookFromSignature<GameUiMessageDelegate>(
                GameUiMessageSignature,
                GameUiMessageDetour
            );
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to initialize simple game UI message hook. {exception.Message}");
            _gameUiMessageHook = null;
        }

        try {
            _parameterizedGameUiMessageHook = _interopProvider.HookFromSignature<ParameterizedGameUiMessageDelegate>(
                ParameterizedGameUiMessageSignature,
                ParameterizedGameUiMessageDetour
            );
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to initialize parameterized game UI message hook. {exception.Message}");
            _parameterizedGameUiMessageHook = null;
        }
    }

    private void TryInitializeOptionalBannerHelperHooks() {
        try {
            _bannerHelperLogCharaCardUpdateResponseHook = _interopProvider.HookFromSignature<BannerHelperLogCharaCardUpdateResponseDelegate>(
                BannerHelperLogCharaCardUpdateResponseSignature,
                BannerHelperLogCharaCardUpdateResponseDetour
            );
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to initialize BannerHelper.LogCharaCardUpdateResponse hook. {exception.Message}");
            _bannerHelperLogCharaCardUpdateResponseHook = null;
        }

        try {
            _bannerHelperOpenCharaCardForPacketHook = _interopProvider.HookFromSignature<BannerHelperOpenCharaCardForPacketDelegate>(
                BannerHelperOpenCharaCardForPacketSignature,
                BannerHelperOpenCharaCardForPacketDetour
            );
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to initialize BannerHelper.OpenCharaCardForPacket hook. {exception.Message}");
            _bannerHelperOpenCharaCardForPacketHook = null;
        }
    }

    private void HandleCurrentCharaCardDataPacketDetour(CharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet) {
        var shouldPropagateOriginal = true;
        try {
            if (!_loggedPacketDetourHit) {
                _loggedPacketDetourHit = true;
                _warningSink?.Invoke("CharaCardResolver: hit HandleCurrentCharaCardDataPacket detour.");
            }
            if (packet != null) {
                shouldPropagateOriginal = _packetHandler?.Invoke(new CharaCardPacketModel(
                    packet->ContentId,
                    packet->WorldId,
                    packet->NameString ?? string.Empty,
                    packet->SomeState,
                    packet->CharaCardData.Data8.Version,
                    packet->CharaCardData.Data8.Flags,
                    packet->CharaCardData.Data8.PrivacyFlags
                )) ?? true;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process packet. {exception.Message}");
        } finally {
            if (shouldPropagateOriginal) {
                _packetHook?.Original(thisPtr, packet);
            }
        }
    }

    private void CharaCardUpdateResponseDispatcherDetour(nint thisPtr, int responseCode, uint responseDetail) {
        try {
            if (!_loggedResponseStatusDetourHit) {
                _loggedResponseStatusDetourHit = true;
                _warningSink?.Invoke(
                    $"CharaCardResolver: hit CharaCard update response dispatcher detour code={responseCode} detail={responseDetail}."
                );
            }
            if (_bannerHelperResponseHandler?.Invoke(new BannerHelperResponseModel(responseCode, responseDetail)) == true) {
                return;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process CharaCard update response dispatcher. {exception.Message}");
        }

        _responseStatusDispatcherHook?.Original(thisPtr, responseCode, responseDetail);
    }

    private void AgentOpenCharaCardForPacketDetour(AgentCharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet, bool a3) {
        var shouldPropagateOriginal = true;
        try {
            if (packet != null) {
                shouldPropagateOriginal = _agentPacketHandler?.Invoke(new CharaCardPacketModel(
                    packet->ContentId,
                    packet->WorldId,
                    packet->NameString ?? string.Empty,
                    packet->SomeState,
                    packet->CharaCardData.Data8.Version,
                    packet->CharaCardData.Data8.Flags,
                    packet->CharaCardData.Data8.PrivacyFlags
                )) ?? true;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process agent packet. {exception.Message}");
        } finally {
            if (shouldPropagateOriginal) {
                _agentPacketHook?.Original(thisPtr, packet, a3);
            }
        }
    }

    private void BannerHelperLogCharaCardUpdateResponseDetour(nint thisPtr, int responseCode, uint responseDetail) {
        try {
            if (_bannerHelperResponseHandler?.Invoke(new BannerHelperResponseModel(responseCode, responseDetail)) == true) {
                return;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process BannerHelper.LogCharaCardUpdateResponse. {exception.Message}");
        }

        _bannerHelperLogCharaCardUpdateResponseHook?.Original(thisPtr, responseCode, responseDetail);
    }

    private void BannerHelperOpenCharaCardForPacketDetour(nint thisPtr, AgentCharaCard.CharaCardPacket* packet) {
        try {
            if (packet != null
                && _bannerHelperPacketHandler?.Invoke(new CharaCardPacketModel(
                    packet->ContentId,
                    packet->WorldId,
                    packet->NameString ?? string.Empty,
                    packet->SomeState,
                    packet->CharaCardData.Data8.Version,
                    packet->CharaCardData.Data8.Flags,
                    packet->CharaCardData.Data8.PrivacyFlags
                )) == true) {
                return;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process BannerHelper.OpenCharaCardForPacket. {exception.Message}");
        }

        _bannerHelperOpenCharaCardForPacketHook?.Original(thisPtr, packet);
    }

    private void GameUiMessageDetour(nint thisPtr, uint messageId) {
        try {
            if (CharaCardResolver.PlateFailureMessageIds.Contains(messageId) && !_loggedSimpleGameUiMessageDetourHit) {
                _loggedSimpleGameUiMessageDetourHit = true;
                _warningSink?.Invoke($"CharaCardResolver: hit simple game UI message detour messageId=0x{messageId:X}.");
            }
            if (_simpleGameUiMessageHandler?.Invoke(new GameUiMessageModel(messageId)) == false) {
                return;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process simple game UI message. {exception.Message}");
        }

        _gameUiMessageHook?.Original(thisPtr, messageId);
    }

    private void ParameterizedGameUiMessageDetour(nint thisPtr, uint messageId, uint param) {
        try {
            if (CharaCardResolver.PlateFailureMessageIds.Contains(messageId) && !_loggedParameterizedGameUiMessageDetourHit) {
                _loggedParameterizedGameUiMessageDetourHit = true;
                _warningSink?.Invoke(
                    $"CharaCardResolver: hit parameterized game UI message detour messageId=0x{messageId:X} param={param}."
                );
            }
            if (_parameterizedGameUiMessageHandler?.Invoke(new GameUiMessageModel(messageId, param, true)) == false) {
                return;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process parameterized game UI message. {exception.Message}");
        }

        _parameterizedGameUiMessageHook?.Original(thisPtr, messageId, param);
    }

    private nint SelectOkStateTransitionDetour(nint thisPtr, nint statePtr, nint eventPtr, nint arg4, int action) {
        try {
            var storage = *(AgentCharaCard.Storage**)(thisPtr + 0x28);
            if (storage != null && !_loggedSelectOkStateTransitionDetourHit) {
                _loggedSelectOkStateTransitionDetourHit = true;
                _warningSink?.Invoke(
                    $"CharaCardResolver: hit SelectOk state transition detour action={action} contentId={storage->ContentId}."
                );
            }
            if (storage != null
                && _selectOkStateTransitionHandler?.Invoke(new SelectOkStateTransitionModel(
                    storage->ContentId,
                    action,
                    storage->CanEdit,
                    storage->IsNotCreated,
                    storage->WasResetDueToFantasia
                )) == false) {
                storage->SelectOkAddonId = 0;
                if (statePtr != 0) {
                    *(int*)statePtr = 2;
                    *((byte*)statePtr + 8) = 0;
                }

                return statePtr;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process SelectOk state transition. {exception.Message}");
        }

        return _selectOkStateTransitionHook?.Original(thisPtr, statePtr, eventPtr, arg4, action) ?? statePtr;
    }

    private uint CreateSelectOkDialogDetour(nint thisPtr, uint messageId, nuint variant) {
        try {
            var storage = *(AgentCharaCard.Storage**)(thisPtr + 0x28);
            if (storage != null && !_loggedFinalSelectOkDialogDetourHit) {
                _loggedFinalSelectOkDialogDetourHit = true;
                _warningSink?.Invoke(
                    $"CharaCardResolver: hit final SelectOk dialog detour messageId=0x{messageId:X} variant={(int)variant} contentId={storage->ContentId}."
                );
            }
            if (storage != null
                && _selectOkDialogHandler?.Invoke(new SelectOkDialogRequestModel(
                    storage->ContentId,
                    messageId,
                    (int)variant,
                    storage->IsNotCreated,
                    storage->WasResetDueToFantasia
                )) == false) {
                return 0;
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process final SelectOk dialog creation. {exception.Message}");
        }

        return _createSelectOkDialogHook?.Original(thisPtr, messageId, variant) ?? 0;
    }
}
