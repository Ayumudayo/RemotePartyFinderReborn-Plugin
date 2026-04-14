using System;
using Dalamud.Game.Chat;
using Dalamud.Game.Addon.Lifecycle;
using Dalamud.Game.Addon.Lifecycle.AddonArgTypes;
using Dalamud.Game.Gui.Toast;
using Dalamud.Hooking;
using Dalamud.Plugin.Services;
using ECommons.DalamudServices;
using FFXIVClientStructs.FFXIV.Client.Game.UI;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;

#nullable enable

namespace RemotePartyFinder;

internal interface ICharaCardResolverRuntime : IDisposable {
    ResolverPreflightResult CheckAvailability();
    void Initialize(
        Func<CharaCardPacketModel, bool> packetHandler,
        Func<CharaCardPacketModel, bool> agentPacketHandler,
        Func<BannerHelperResponseModel, bool> responseDispatcherHandler,
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

internal sealed class DalamudSelectOkDialogSuppressionRuntime : ISelectOkDialogSuppressionRuntime {
    private readonly IChatGui _chatGui;
    private readonly IToastGui _toastGui;
    private readonly Action<string>? _warningSink;
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
        _ = addonLifecycle ?? throw new ArgumentNullException(nameof(addonLifecycle));
        _chatGui = chatGui ?? throw new ArgumentNullException(nameof(chatGui));
        _toastGui = toastGui ?? throw new ArgumentNullException(nameof(toastGui));
        _warningSink = warningSink;
        _logMessageHandler = OnLogMessage;
        _normalToastHandler = OnNormalToast;
        _questToastHandler = OnQuestToast;
        _errorToastHandler = OnErrorToast;
    }

    public void Initialize(Func<string, bool> selectOkHandler) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _selectOkHandler = selectOkHandler ?? throw new ArgumentNullException(nameof(selectOkHandler));

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
        _chatGui.LogMessage -= _logMessageHandler;
        _toastGui.Toast -= _normalToastHandler;
        _toastGui.QuestToast -= _questToastHandler;
        _toastGui.ErrorToast -= _errorToastHandler;
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
    private Func<BannerHelperResponseModel, bool>? _responseDispatcherHandler;
    private Func<SelectOkStateTransitionModel, bool>? _selectOkStateTransitionHandler;
    private Func<GameUiMessageModel, bool>? _simpleGameUiMessageHandler;
    private Func<GameUiMessageModel, bool>? _parameterizedGameUiMessageHandler;
    private Func<SelectOkDialogRequestModel, bool>? _selectOkDialogHandler;

    private delegate void HandleCurrentCharaCardDataPacketDelegate(CharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet);
    private delegate void CharaCardUpdateResponseDispatcherDelegate(nint thisPtr, int responseCode, uint responseDetail);
    private delegate void AgentOpenCharaCardForPacketDelegate(AgentCharaCard* thisPtr, AgentCharaCard.CharaCardPacket* packet, bool a3);
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
        Func<BannerHelperResponseModel, bool> responseDispatcherHandler,
        Func<SelectOkStateTransitionModel, bool> selectOkStateTransitionHandler,
        Func<GameUiMessageModel, bool> simpleGameUiMessageHandler,
        Func<GameUiMessageModel, bool> parameterizedGameUiMessageHandler,
        Func<SelectOkDialogRequestModel, bool> selectOkDialogHandler
    ) {
        ArgumentNullException.ThrowIfNull(packetHandler);
        ArgumentNullException.ThrowIfNull(agentPacketHandler);
        ArgumentNullException.ThrowIfNull(responseDispatcherHandler);
        ArgumentNullException.ThrowIfNull(selectOkStateTransitionHandler);
        ArgumentNullException.ThrowIfNull(simpleGameUiMessageHandler);
        ArgumentNullException.ThrowIfNull(parameterizedGameUiMessageHandler);
        ArgumentNullException.ThrowIfNull(selectOkDialogHandler);

        _packetHandler = packetHandler;
        _agentPacketHandler = agentPacketHandler;
        _responseDispatcherHandler = responseDispatcherHandler;
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
        _packetHook.Enable();
        _responseStatusDispatcherHook.Enable();
        _agentPacketHook.Enable();
        _selectOkStateTransitionHook?.Enable();
        _gameUiMessageHook?.Enable();
        _parameterizedGameUiMessageHook?.Enable();
        _createSelectOkDialogHook.Enable();
        _warningSink?.Invoke(
            "CharaCardResolver: hook init " +
            $"packet=true responseDispatcher=true agent=true stateTransition={_selectOkStateTransitionHook is not null} " +
            $"simpleMessage={_gameUiMessageHook is not null} parameterizedMessage={_parameterizedGameUiMessageHook is not null} " +
            "finalSelectOk=true bannerLog=false bannerPacket=false"
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
        _responseDispatcherHandler = null;
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
            if (_responseDispatcherHandler?.Invoke(new BannerHelperResponseModel(responseCode, responseDetail)) == true) {
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
