using System;
using System.Collections.Generic;
using System.Linq;
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
    string Name
);

internal interface ICharaCardResolverRuntime : IDisposable {
    ResolverPreflightResult CheckAvailability();
    void Initialize(Action<CharaCardPacketModel> packetHandler);
    bool TryRequest(ulong contentId);
}

internal sealed class CharaCardResolver : IDisposable {
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(5);

    private readonly PlayerLocalDatabase _database;
    private readonly ContentIdResolveQueue _queue = new();
    private readonly ICharaCardResolverRuntime _runtime;
    private readonly Func<ushort, string?> _worldNameResolver;
    private readonly Func<DateTime> _utcNow;
    private readonly Action<CharacterIdentitySnapshot> _persistResolvedIdentity;
    private readonly TimeSpan _requestTimeout;
    private readonly Action<string>? _warningSink;
    private readonly Dictionary<ulong, PendingProjection> _pendingProjections = new();
    private readonly object _sync = new();
    private bool _disposed;

    internal CharaCardResolver(
        PlayerLocalDatabase database,
        ICharaCardResolverRuntime? runtime = null,
        Func<ushort, string?>? worldNameResolver = null,
        Func<DateTime>? utcNow = null,
        Action<string>? warningSink = null,
        Action<CharacterIdentitySnapshot>? persistResolvedIdentity = null,
        TimeSpan? requestTimeout = null
    ) {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _warningSink = warningSink;
        _runtime = runtime ?? new DalamudCharaCardResolverRuntime(warningSink);
        _worldNameResolver = worldNameResolver ?? ResolveWorldName;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _persistResolvedIdentity = persistResolvedIdentity ?? _database.UpsertResolvedIdentity;
        _requestTimeout = requestTimeout ?? DefaultRequestTimeout;

        Preflight = _runtime.CheckAvailability();
        if (!Preflight.Enabled) {
            LogWarning($"CharaCardResolver: disabled during preflight. {Preflight.Reason}");
            return;
        }

        try {
            _runtime.Initialize(HandlePacket);
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
        if (_disposed || !IsEnabled) {
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
                lock (_sync) {
                    _queue.MarkTimeout(request.ContentId, request.AttemptVersion, _utcNow());
                }
            }
        } catch (Exception exception) {
            DisableEnrichment($"request dispatch failed: {exception.Message}");
        }
    }

    internal ResolveState GetResolveState(ulong contentId) {
        lock (_sync) {
            return _queue.GetState(contentId);
        }
    }

    internal static unsafe ResolverPreflightResult CheckAvailability() {
        return ResolverPreflightEvaluator.Evaluate(
            (nint)CharaCard.MemberFunctionPointers.RequestCharaCardForContentId,
            (nint)CharaCard.MemberFunctionPointers.HandleCurrentCharaCardDataPacket
        );
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
        _runtime.Dispose();
    }

    private void HandlePacket(CharaCardPacketModel packet) {
        if (_disposed || !IsEnabled) {
            return;
        }

        if (!TryProjectIdentity(packet, _worldNameResolver, _utcNow(), out var snapshot)) {
            return;
        }

        lock (_sync) {
            var request = _queue.GetRequest(snapshot.ContentId);
            _pendingProjections[snapshot.ContentId] = new PendingProjection(snapshot, request.AttemptVersion);
        }
    }

    private void DisableEnrichment(string reason) {
        IsEnabled = false;
        LogWarning($"CharaCardResolver: disabled. {reason}");

        try {
            _runtime.Dispose();
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to dispose runtime after disable. {exception.Message}");
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

            _queue.MarkTimeout(request.ContentId, request.AttemptVersion, nowUtc);
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
        } catch (Exception exception) {
            LogWarning($"CharaCardResolver: failed to persist resolved identity for {candidate.Snapshot.ContentId}. {exception.Message}");
            lock (_sync) {
                _pendingProjections.Remove(candidate.Snapshot.ContentId);
                _queue.MarkTimeout(candidate.Snapshot.ContentId, candidate.AttemptVersion, nowUtc);
            }
        }

        return true;
    }

    private sealed record PendingProjection(
        CharacterIdentitySnapshot Snapshot,
        int AttemptVersion
    );
}

internal unsafe sealed class DalamudCharaCardResolverRuntime : ICharaCardResolverRuntime {
    private readonly IGameInteropProvider _interopProvider;
    private readonly delegate*<CharaCard*> _instanceResolver;
    private readonly Action<string>? _warningSink;
    private Hook<HandleCurrentCharaCardDataPacketDelegate>? _packetHook;
    private Action<CharaCardPacketModel>? _packetHandler;

    private delegate void HandleCurrentCharaCardDataPacketDelegate(CharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet);

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

    public void Initialize(Action<CharaCardPacketModel> packetHandler) {
        ArgumentNullException.ThrowIfNull(packetHandler);

        _packetHandler = packetHandler;
        _packetHook = _interopProvider.HookFromAddress<HandleCurrentCharaCardDataPacketDelegate>(
            (nint)CharaCard.MemberFunctionPointers.HandleCurrentCharaCardDataPacket,
            HandleCurrentCharaCardDataPacketDetour
        );
        _packetHook.Enable();
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
        _packetHook?.Dispose();
        _packetHook = null;
    }

    private void HandleCurrentCharaCardDataPacketDetour(CharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet) {
        try {
            if (packet != null) {
                _packetHandler?.Invoke(new CharaCardPacketModel(
                    packet->ContentId,
                    packet->WorldId,
                    packet->NameString ?? string.Empty
                ));
            }
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to process packet. {exception.Message}");
        } finally {
            _packetHook?.Original(thisPtr, packet);
        }
    }
}
