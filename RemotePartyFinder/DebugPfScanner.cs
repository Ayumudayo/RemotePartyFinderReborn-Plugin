using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using ECommons.Automation.UIInput;
using Dalamud.Game.Addon.Lifecycle;
using Dalamud.Game.Addon.Lifecycle.AddonArgTypes;
using Dalamud.Game.Agent;
using Dalamud.Game.Agent.AgentArgTypes;
using Dalamud.Game.Gui.PartyFinder.Types;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;
using FFXIVClientStructs.FFXIV.Component.GUI;
using AtkValueType = FFXIVClientStructs.FFXIV.Component.GUI.ValueType;

namespace RemotePartyFinder;

internal sealed class DebugPfScanner : IDisposable {
    private const int MinCollectionTimeoutMs = 1500;
    private const int CollectionTimeoutExtraBudgetMs = 700;
    private const int MaxCapturedAtkValues = 16;
    private static readonly TimeSpan CaptureArmTimeout = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan CaptureEventParamWindow = TimeSpan.FromSeconds(3);
    private static readonly AtkEventType[] NextButtonResolveEventTypes = {
        AtkEventType.ButtonClick,
        AtkEventType.MouseClick,
        AtkEventType.ButtonPress,
        AtkEventType.ButtonRelease,
    };

    private readonly Plugin _plugin;
    private readonly PartyDetailCollector _detailCollector;
    private readonly Gatherer _gatherer;
    private readonly ConcurrentQueue<ListingCandidate> _incoming = new();
    private readonly Dictionary<uint, ListingCandidate> _latestVisible = new();
    private readonly Dictionary<uint, DateTime> _attemptedAt = new();
    private readonly Queue<ListingCandidate> _pending = new();
    private readonly Stopwatch _tickGate = new();

    private ScanState _state = ScanState.Idle;
    private ListingCandidate _target;
    private bool _hasTarget;
    private bool _retryTargetAfterCooldown;
    private int _openAttemptsForTarget;
    private int _pageAdvanceNotReadyCount;
    private DateTime _noReadySinceUtc = DateTime.MinValue;
    private int _consecutiveFailures;
    private int _processedCount;
    private DateTime _nextActionAtUtc = DateTime.MinValue;
    private DateTime _nextRefreshAtUtc = DateTime.MinValue;
    private DateTime _stateDeadlineUtc = DateTime.MinValue;
    private ulong _pageReloadBaselineHash;
    private bool _pageReloadRefreshRequested;
    private bool _nextPagePreflightRequested;
    private long _waitForQueueAckVersion = -1;
    private long _waitForAckVersion = -1;
    private long _waitForTerminalAckVersion = -1;
    private DetailSnapshot _lastReadySnapshot;
    private int _readyStableTicks;
    private string _lastAttemptReason = "none";
    private bool _lastAttemptSuccess;
    private uint _lastAttemptListingId;
    private bool _wasEnabled;
    private bool _noMorePagesDetected;
    private bool _captureNextPageEventArmed;
    private DateTime _captureArmedAtUtc = DateTime.MinValue;
    private bool _isReplayingNextPageEvent;
    private CapturedReceiveEvent? _capturedNextPageEvent;
    private int _capturedNextPageButtonId = -1;
    private DateTime _capturedNextPageEventAtUtc = DateTime.MinValue;
    private string _lastObservedReceiveEvent = "none";
    private string _lastObservedAddonReceiveEvent = "none";
    private int _recentAddonEventParam = -1;
    private DateTime _recentAddonEventAtUtc = DateTime.MinValue;
    private int _recentAddonButtonEventParam = -1;
    private DateTime _recentAddonButtonEventAtUtc = DateTime.MinValue;
    private DateTime _autoHydrateNextPageButtonIdUntilUtc = DateTime.MinValue;
    private DateTime _autoCaptureNextPageEventUntilUtc = DateTime.MinValue;
    private DateTime _nextButtonResolveAtUtc = DateTime.MinValue;

    internal DebugPfScanner(Plugin plugin, PartyDetailCollector detailCollector, Gatherer gatherer) {
        _plugin = plugin;
        _detailCollector = detailCollector;
        _gatherer = gatherer;

        _plugin.PartyFinderGui.ReceiveListing += OnListing;
        _plugin.AddonLifecycle.RegisterListener(AddonEvent.PostReceiveEvent, "LookingForGroup", OnLookingForGroupAddonReceiveEvent);
        _plugin.AgentLifecycle.RegisterListener(AgentEvent.PostReceiveEvent, Dalamud.Game.Agent.AgentId.LookingForGroup, OnLookingForGroupReceiveEvent);
        _plugin.AgentLifecycle.RegisterListener(AgentEvent.PostReceiveEventWithResult, Dalamud.Game.Agent.AgentId.LookingForGroup, OnLookingForGroupReceiveEvent);
        _plugin.Framework.Update += OnUpdate;
        _tickGate.Start();
    }

    internal bool Enabled => _plugin.Configuration.EnableAutoDetailScanDebug;
    internal string StateName => _state.ToString();
    internal uint CurrentTargetListingId => _hasTarget ? _target.ListingId : 0;
    internal int PendingCount => _pending.Count;
    internal int ProcessedCount => _processedCount;
    internal int ConsecutiveFailures => _consecutiveFailures;
    internal int VisibleListingCount => _latestVisible.Count;
    internal string LastAttemptReason => _lastAttemptReason;
    internal bool LastAttemptSuccess => _lastAttemptSuccess;
    internal uint LastAttemptListingId => _lastAttemptListingId;
    internal bool IsNextPageCaptureArmed => _captureNextPageEventArmed;
    internal bool HasNextPageCapture => _capturedNextPageEvent.HasValue;
    internal ulong CapturedNextPageEventKind => _capturedNextPageEvent?.EventKind ?? 0;
    internal int CapturedNextPageValueCount => _capturedNextPageEvent?.Values.Length ?? 0;
    internal int CapturedNextPageActionId => _capturedNextPageEvent.HasValue && TryReadPrimaryActionId(_capturedNextPageEvent.Value.Values, out var actionId) ? actionId : -1;
    internal int CapturedNextPageButtonId => _capturedNextPageButtonId;
    internal bool CapturedNextPageUsesWithResult => _capturedNextPageEvent?.UsesWithResult ?? false;
    internal string CapturedNextPageValues => _capturedNextPageEvent.HasValue ? DescribeValues(_capturedNextPageEvent.Value.Values) : "none";
    internal string LastObservedReceiveEvent => _lastObservedReceiveEvent;
    internal string LastObservedAddonReceiveEvent => _lastObservedAddonReceiveEvent;

    public void Dispose() {
        _plugin.Framework.Update -= OnUpdate;
        _plugin.PartyFinderGui.ReceiveListing -= OnListing;
        _plugin.AddonLifecycle.UnregisterListener(AddonEvent.PostReceiveEvent, "LookingForGroup", OnLookingForGroupAddonReceiveEvent);
        _plugin.AgentLifecycle.UnregisterListener(AgentEvent.PostReceiveEvent, Dalamud.Game.Agent.AgentId.LookingForGroup, OnLookingForGroupReceiveEvent);
        _plugin.AgentLifecycle.UnregisterListener(AgentEvent.PostReceiveEventWithResult, Dalamud.Game.Agent.AgentId.LookingForGroup, OnLookingForGroupReceiveEvent);
    }

    internal void ResetSession() {
        _incoming.Clear();
        _latestVisible.Clear();
        _attemptedAt.Clear();
        _pending.Clear();

        _state = ScanState.Idle;
        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
        _pageAdvanceNotReadyCount = 0;
        _noReadySinceUtc = DateTime.MinValue;
        _consecutiveFailures = 0;
        _processedCount = 0;
        _nextActionAtUtc = DateTime.MinValue;
        _nextRefreshAtUtc = DateTime.MinValue;
        _stateDeadlineUtc = DateTime.MinValue;
        _pageReloadBaselineHash = 0;
        _pageReloadRefreshRequested = false;
        _nextPagePreflightRequested = false;
        _waitForQueueAckVersion = -1;
        _waitForAckVersion = -1;
        _waitForTerminalAckVersion = -1;
        _lastReadySnapshot = default;
        _readyStableTicks = 0;
        _lastAttemptReason = "none";
        _lastAttemptSuccess = false;
        _lastAttemptListingId = 0;
        _noMorePagesDetected = false;
        _capturedNextPageEventAtUtc = DateTime.MinValue;
        _lastObservedAddonReceiveEvent = "none";
        _recentAddonEventParam = -1;
        _recentAddonEventAtUtc = DateTime.MinValue;
        _recentAddonButtonEventParam = -1;
        _recentAddonButtonEventAtUtc = DateTime.MinValue;
        _autoHydrateNextPageButtonIdUntilUtc = DateTime.MinValue;
        _autoCaptureNextPageEventUntilUtc = DateTime.MinValue;
        _nextButtonResolveAtUtc = DateTime.MinValue;
    }

    internal void ArmNextPageCapture() {
        _captureNextPageEventArmed = true;
        _captureArmedAtUtc = DateTime.UtcNow;
        _recentAddonEventParam = -1;
        _recentAddonEventAtUtc = DateTime.MinValue;
        _recentAddonButtonEventParam = -1;
        _recentAddonButtonEventAtUtc = DateTime.MinValue;
        Plugin.Log.Debug("DebugPfScanner: armed next-page event capture");
    }

    internal void ClearNextPageCapture() {
        _captureNextPageEventArmed = false;
        _captureArmedAtUtc = DateTime.MinValue;
        _capturedNextPageEvent = null;
        _capturedNextPageButtonId = -1;
        _capturedNextPageEventAtUtc = DateTime.MinValue;
        _lastObservedReceiveEvent = "none";
        _lastObservedAddonReceiveEvent = "none";
        _recentAddonEventParam = -1;
        _recentAddonEventAtUtc = DateTime.MinValue;
        _recentAddonButtonEventParam = -1;
        _recentAddonButtonEventAtUtc = DateTime.MinValue;
        _autoHydrateNextPageButtonIdUntilUtc = DateTime.MinValue;
        _autoCaptureNextPageEventUntilUtc = DateTime.MinValue;
        _nextButtonResolveAtUtc = DateTime.MinValue;
        Plugin.Log.Debug("DebugPfScanner: cleared captured next-page event");
    }

    private void OnListing(IPartyFinderListing listing, IPartyFinderListingEventArgs args) {
        if (!Enabled || listing.Id == 0) {
            return;
        }

        _incoming.Enqueue(new ListingCandidate(
            listing.Id,
            listing.ContentId,
            DateTime.UtcNow,
            args.BatchNumber
        ));
    }

    private unsafe void OnLookingForGroupAddonReceiveEvent(AddonEvent eventType, AddonArgs args) {
        if (args is not AddonReceiveEventArgs receiveEventArgs) {
            return;
        }

        var now = DateTime.UtcNow;
        var eventParam = receiveEventArgs.EventParam;
        var hasButtonNodeId = TryExtractButtonNodeIdFromAddonReceiveEvent(receiveEventArgs, out var buttonNodeId, out var addonEventDetail);
        _lastObservedAddonReceiveEvent = $"atk_type={receiveEventArgs.AtkEventType} event_param={eventParam} {addonEventDetail}";

        var allowReplayAddonSampling = _isReplayingNextPageEvent && now <= _autoHydrateNextPageButtonIdUntilUtc;
        if (_isReplayingNextPageEvent && !allowReplayAddonSampling) {
            return;
        }

        if (eventParam > 0 && eventParam <= 1000) {
            _recentAddonEventParam = eventParam;
            _recentAddonEventAtUtc = now;
        }

        var atkEventType = (int)receiveEventArgs.AtkEventType;
        if (atkEventType is (int)AtkEventType.ButtonClick
            or (int)AtkEventType.MouseClick
            or (int)AtkEventType.ButtonPress
            or (int)AtkEventType.ButtonRelease) {
            if (hasButtonNodeId && buttonNodeId > 0) {
                _recentAddonButtonEventParam = buttonNodeId;
                _recentAddonButtonEventAtUtc = now;
            }

            if (_capturedNextPageButtonId <= 0
                && _plugin.Configuration.AutoDetailScanNextPageButtonId <= 0
                && now <= _autoHydrateNextPageButtonIdUntilUtc) {
                var hydratedButtonId = ResolveCapturedNextPageButtonId();
                if (hydratedButtonId > 0) {
                    _capturedNextPageButtonId = hydratedButtonId;
                    _autoHydrateNextPageButtonIdUntilUtc = DateTime.MinValue;
                    Plugin.Log.Debug($"DebugPfScanner: auto-hydrated next-page button id={hydratedButtonId} from callback-triggered addon button event.");
                }
            }
        }

        TryHydrateCapturedNextPageButtonId(now);
    }

    private unsafe void OnLookingForGroupReceiveEvent(AgentEvent eventType, AgentArgs args) {
        var now = DateTime.UtcNow;
        var allowReplayCapture = _isReplayingNextPageEvent && now <= _autoCaptureNextPageEventUntilUtc;
        if (_isReplayingNextPageEvent && !allowReplayCapture) {
            return;
        }

        if (args is not AgentReceiveEventArgs receiveEventArgs) {
            return;
        }

        if (receiveEventArgs.ValueCount == 0 || receiveEventArgs.ValueCount > MaxCapturedAtkValues || receiveEventArgs.AtkValues == 0) {
            return;
        }

        if (!IsLookingForGroupOpen()) {
            return;
        }

        var valuesPtr = (AtkValue*)receiveEventArgs.AtkValues;
        _lastObservedReceiveEvent = $"kind={receiveEventArgs.EventKind} values={receiveEventArgs.ValueCount} [{DescribeValues(valuesPtr, receiveEventArgs.ValueCount)}]";

        var autoCaptureArmed = now <= _autoCaptureNextPageEventUntilUtc;
        if (!_captureNextPageEventArmed && !autoCaptureArmed) {
            return;
        }

        if (!TryReadPrimaryActionId(valuesPtr, receiveEventArgs.ValueCount, out var actionId)) {
            if (_captureNextPageEventArmed) {
                Plugin.Log.Debug($"DebugPfScanner: skipped next-page capture event kind={receiveEventArgs.EventKind} reason=no_primary_action");
            }
            return;
        }

        var expectedActionId = Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageActionId, 0, 1000);
        if (actionId != expectedActionId) {
            if (_captureNextPageEventArmed) {
                Plugin.Log.Debug($"DebugPfScanner: ignored capture event action={actionId} expected={expectedActionId} kind={receiveEventArgs.EventKind}");
            }
            return;
        }

        if (!TryCopyCaptureValues(valuesPtr, receiveEventArgs.ValueCount, out var values, out var reason)) {
            if (_captureNextPageEventArmed) {
                Plugin.Log.Debug($"DebugPfScanner: skipped next-page capture event kind={receiveEventArgs.EventKind} reason={reason}");
            }
            return;
        }

        var usesWithResult = eventType == AgentEvent.PostReceiveEventWithResult;
        _capturedNextPageEventAtUtc = now;
        var capturedButtonId = ResolveCapturedNextPageButtonId();
        if (capturedButtonId <= 0) {
            _ = TryInferCapturedNextPageButtonId(values, out capturedButtonId);
        }

        _capturedNextPageEvent = new CapturedReceiveEvent(receiveEventArgs.EventKind, usesWithResult, values);
        if (capturedButtonId > 0) {
            _capturedNextPageButtonId = capturedButtonId;
        }

        if (_captureNextPageEventArmed) {
            _captureNextPageEventArmed = false;
            _captureArmedAtUtc = DateTime.MinValue;
        }

        if (autoCaptureArmed) {
            _autoCaptureNextPageEventUntilUtc = DateTime.MinValue;
        }

        if (capturedButtonId > 0) {
            Plugin.Log.Debug($"DebugPfScanner: captured next-page event action={actionId} button_id={capturedButtonId} kind={receiveEventArgs.EventKind} with_result={usesWithResult} auto={autoCaptureArmed} value_count={values.Length} values=[{DescribeValues(values)}]");
        } else {
            Plugin.Log.Debug($"DebugPfScanner: captured next-page event action={actionId} button_id=unknown kind={receiveEventArgs.EventKind} with_result={usesWithResult} auto={autoCaptureArmed} value_count={values.Length} values=[{DescribeValues(values)}]; waiting for addon event hydration.");
        }
    }

    private unsafe bool TryExtractButtonNodeIdFromAddonReceiveEvent(AddonReceiveEventArgs receiveEventArgs, out int buttonNodeId, out string detail) {
        buttonNodeId = -1;
        detail = "node=none";

        if (receiveEventArgs.AtkEvent == 0) {
            return false;
        }

        var atkEvent = (AtkEvent*)receiveEventArgs.AtkEvent;
        if (atkEvent == null || atkEvent->Node == null) {
            detail = "node=null";
            return false;
        }

        var node = atkEvent->Node;
        var nodeId = (int)node->NodeId;
        var nodeType = node->Type;
        detail = $"node_id={nodeId} node_type={nodeType}";
        if (nodeId <= 0 || nodeType != NodeType.Component) {
            return false;
        }

        var componentNode = node->GetAsAtkComponentNode();
        if (componentNode == null || componentNode->Component == null) {
            detail += " component=none";
            return false;
        }

        var componentType = componentNode->Component->GetComponentType();
        detail += $" component_type={componentType}";
        if (componentType is ComponentType.Button
            or ComponentType.CheckBox
            or ComponentType.RadioButton
            or ComponentType.HoldButton) {
            buttonNodeId = nodeId;
            return true;
        }

        return false;
    }

    private bool TryInferCapturedNextPageButtonId(CapturedAtkValue[] values, out int buttonId) {
        buttonId = -1;
        var candidates = new HashSet<int>();
        var expectedActionId = Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageActionId, 0, 1000);

        for (var index = 1; index < values.Length; index++) {
            if (!TryConvertToActionId(values[index].Type, values[index].Raw, out var candidate)) {
                continue;
            }

            if (candidate <= 0 || candidate > 1000) {
                continue;
            }

            if (candidate == expectedActionId) {
                continue;
            }

            if (!TryReadLookingForGroupButtonEnabled((uint)candidate, out _)) {
                continue;
            }

            candidates.Add(candidate);
        }

        if (candidates.Count == 0) {
            return false;
        }

        if (candidates.Count == 1) {
            buttonId = candidates.First();
            return true;
        }

        if (_recentAddonButtonEventParam > 0 && candidates.Contains(_recentAddonButtonEventParam)) {
            buttonId = _recentAddonButtonEventParam;
            return true;
        }

        return false;
    }

    private void OnUpdate(IFramework framework) {
        if (_tickGate.ElapsedMilliseconds < 50) {
            return;
        }

        _tickGate.Restart();

        if (_captureNextPageEventArmed && DateTime.UtcNow - _captureArmedAtUtc > CaptureArmTimeout) {
            _captureNextPageEventArmed = false;
            _captureArmedAtUtc = DateTime.MinValue;
            Plugin.Log.Debug("DebugPfScanner: next-page capture timed out; arm again and click Next page manually.");
        }

        if (!Enabled) {
            if (_state != ScanState.Idle || _pending.Count > 0 || _processedCount > 0 || _consecutiveFailures > 0) {
                ResetSession();
            }
            _wasEnabled = false;
            return;
        }

        if (!_wasEnabled) {
            _wasEnabled = true;
            StartRunFromFirstPage();
            return;
        }

        if (!IsLookingForGroupOpen()) {
            SetIdle();
            return;
        }

        var maxFailures = Math.Max(_plugin.Configuration.AutoDetailScanMaxConsecutiveFailures, 1);
        if (_consecutiveFailures >= maxFailures) {
            return;
        }

        switch (_state) {
            case ScanState.Idle:
                _state = ScanState.SyncQueue;
                break;

            case ScanState.SyncQueue:
                SyncQueue();
                break;

            case ScanState.OpenTarget:
                OpenTarget();
                break;

            case ScanState.WaitDetailReady:
                WaitDetailReady();
                break;

            case ScanState.WaitCollected:
                WaitCollected();
                break;

            case ScanState.WaitPageReload:
                WaitPageReload();
                break;

            case ScanState.Cooldown:
                if (DateTime.UtcNow >= _nextActionAtUtc) {
                    if (_retryTargetAfterCooldown && _hasTarget) {
                        _retryTargetAfterCooldown = false;
                        _state = ScanState.OpenTarget;
                    } else {
                        _state = ScanState.SyncQueue;
                    }
                }
                break;
        }
    }

    private void SyncQueue() {
        DrainIncoming();
        PruneCaches();

        // In multi-page mode we now advance pages explicitly.
        // Keep queue candidates sourced from real ReceiveListing events only,
        // otherwise synthetic listing IDs can stay permanently "not ready" and
        // trap the scanner in refresh loops.

        if (_plugin.Configuration.AutoDetailScanMaxPerRun > 0
            && _processedCount >= _plugin.Configuration.AutoDetailScanMaxPerRun) {
            CompleteRun("max_per_run");
            return;
        }

        if (_pending.Count == 0) {
            RebuildPendingQueue();
        }

        if (!TryTakeNextReadyTarget(out var nextTarget)) {
            var now = DateTime.UtcNow;

            if (_pending.Count > 0 && _incoming.IsEmpty && !_plugin.Configuration.AutoDetailScanCurrentPageOnly) {
                if (_noReadySinceUtc == DateTime.MinValue) {
                    _noReadySinceUtc = now;
                }

                var noReadyElapsed = now - _noReadySinceUtc;
                if (noReadyElapsed >= TimeSpan.FromSeconds(8)) {
                    var stalePageAdvance = TryAdvanceToNextPage();
                    if (stalePageAdvance == PageAdvanceResult.Advanced) {
                        _pageAdvanceNotReadyCount = 0;
                        _noReadySinceUtc = DateTime.MinValue;
                        return;
                    }
                }
            } else {
                _noReadySinceUtc = DateTime.MinValue;
            }

            if (_pending.Count > 0 || !_incoming.IsEmpty) {
                _nextActionAtUtc = now.AddMilliseconds(250);
                _state = ScanState.Cooldown;
                return;
            }

            if (_noMorePagesDetected) {
                if (_detailCollector.PendingQueueCount == 0) {
                    CompleteRun("no_more_pages");
                    return;
                }

                _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
                _state = ScanState.Cooldown;
                return;
            }

            var pageAdvanceResult = TryAdvanceToNextPage();
            if (pageAdvanceResult == PageAdvanceResult.Advanced) {
                _pageAdvanceNotReadyCount = 0;
                return;
            }

            if (pageAdvanceResult == PageAdvanceResult.NotReady) {
                _pageAdvanceNotReadyCount++;
                if (_pageAdvanceNotReadyCount >= 6) {
                    if (_detailCollector.PendingQueueCount == 0) {
                        CompleteRun("next_page_unavailable");
                        return;
                    }

                    _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
                    _state = ScanState.Cooldown;
                    return;
                }

                _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
                _state = ScanState.Cooldown;
                return;
            }

            _pageAdvanceNotReadyCount = 0;
            _noReadySinceUtc = DateTime.MinValue;
            _noMorePagesDetected = true;

            if (_detailCollector.PendingQueueCount == 0) {
                CompleteRun("no_more_pages");
                return;
            }

            _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
            _state = ScanState.Cooldown;
            return;
        }

        _target = nextTarget;
        _hasTarget = true;
        _openAttemptsForTarget = 0;
        _retryTargetAfterCooldown = false;
        _noReadySinceUtc = DateTime.MinValue;
        _state = ScanState.OpenTarget;
    }

    private bool TryTakeNextReadyTarget(out ListingCandidate target) {
        var remaining = _pending.Count;
        while (remaining-- > 0) {
            var candidate = _pending.Dequeue();

            if (IsRecentlyAttempted(candidate.ListingId)) {
                continue;
            }

            if (!IsListingReadyForDetail(candidate)) {
                _pending.Enqueue(candidate);
                continue;
            }

            target = candidate;
            return true;
        }

        target = default;
        return false;
    }

    private bool IsListingReadyForDetail(ListingCandidate candidate) {
        return _gatherer.WasListingUploadedAfter(candidate.ListingId, candidate.SeenAtUtc);
    }

    private void OpenTarget() {
        if (!_hasTarget) {
            _state = ScanState.SyncQueue;
            return;
        }

        if (DateTime.UtcNow < _nextActionAtUtc) {
            return;
        }

        var opened = TryOpenTarget(_target);
        _openAttemptsForTarget++;

        var actionIntervalMs = Math.Clamp(_plugin.Configuration.AutoDetailScanActionIntervalMs, 100, 2000);
        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(actionIntervalMs);

        if (opened) {
            var timeoutMs = GetDetailReadyTimeoutMs();
            _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
            _waitForQueueAckVersion = _detailCollector.LastQueuedAckVersion;
            _waitForAckVersion = _detailCollector.LastSuccessfulUploadAckVersion;
            _waitForTerminalAckVersion = _detailCollector.LastTerminalUploadAckVersion;
            _state = ScanState.WaitDetailReady;
            return;
        }

        if (_openAttemptsForTarget < 2) {
            _retryTargetAfterCooldown = true;
            _state = ScanState.Cooldown;
            return;
        }

        MarkAttempt(false, "open_failed");
    }

    private unsafe bool TryOpenTarget(ListingCandidate target) {
        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return false;
        }

        if (agent->OpenListing(target.ListingId)) {
            return true;
        }

        return target.ContentId != 0 && agent->OpenListingByContentId(target.ContentId);
    }

    private void WaitDetailReady() {
        if (!_hasTarget) {
            _state = ScanState.SyncQueue;
            return;
        }

        var snapshot = GetCurrentDetailSnapshot();
        var listingMatches = snapshot.ListingId == _target.ListingId;
        var leaderMatches = _target.ContentId == 0 || snapshot.LeaderContentId == _target.ContentId;
        var hasMembers = snapshot.NonZeroMembers > 0;

        if (listingMatches && leaderMatches && hasMembers) {
            if (_readyStableTicks > 0 && snapshot.Equals(_lastReadySnapshot)) {
                _readyStableTicks++;
            } else {
                _lastReadySnapshot = snapshot;
                _readyStableTicks = 1;
            }

            var requiredStableTicks = _target.ContentId == 0 ? 4 : 2;
            if (_readyStableTicks >= requiredStableTicks) {
                var minDwellMs = Math.Clamp(_plugin.Configuration.AutoDetailScanMinDwellMs, 100, 5000);
                var timeoutMs = GetDetailCollectionTimeoutMs(minDwellMs);
                _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(timeoutMs);
                _state = ScanState.WaitCollected;
                return;
            }
        } else {
            _readyStableTicks = 0;
            _lastReadySnapshot = default;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        if (_openAttemptsForTarget < 2) {
            _retryTargetAfterCooldown = true;
            _state = ScanState.Cooldown;
            return;
        }

        MarkAttempt(false, "detail_timeout");
    }

    private void WaitCollected() {
        if (!_hasTarget) {
            _state = ScanState.SyncQueue;
            return;
        }

        var queueAckVersion = _detailCollector.LastQueuedAckVersion;
        var hasQueuedAck = queueAckVersion > _waitForQueueAckVersion
                           && _detailCollector.LastUploadedListingId == _target.ListingId;

        if (hasQueuedAck) {
            MarkAttempt(true, "queued");
            return;
        }

        var ackVersion = _detailCollector.LastSuccessfulUploadAckVersion;
        var hasAppliedAck = ackVersion > _waitForAckVersion
                            && _detailCollector.LastSuccessfulUploadListingId == _target.ListingId;
        var terminalAckVersion = _detailCollector.LastTerminalUploadAckVersion;
        var hasTerminalAck = terminalAckVersion > _waitForTerminalAckVersion
                             && _detailCollector.LastTerminalUploadListingId == _target.ListingId;

        if (hasAppliedAck) {
            MarkAttempt(true, "collected");
            return;
        }

        if (hasTerminalAck) {
            MarkAttempt(true, "listing_missing");
            return;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        MarkAttempt(false, "collector_timeout");
    }

    private void WaitPageReload() {
        DrainIncoming();
        PruneCaches();

        var configuredFallbackButtonId = (uint)Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageButtonId, 0, 1000);
        var resolvedButtonId = ResolveNextPageButtonId(configuredFallbackButtonId);
        if (resolvedButtonId > 0
            && TryReadLookingForGroupButtonEnabled(resolvedButtonId, out var nextButtonEnabled)
            && !nextButtonEnabled) {
            _noMorePagesDetected = true;
            Plugin.Log.Debug($"DebugPfScanner: detected last page while waiting for reload (button_id={resolvedButtonId}, baseline_hash=0x{_pageReloadBaselineHash:X16}).");

            if (_detailCollector.PendingQueueCount == 0) {
                CompleteRun("no_more_pages_button_disabled");
                return;
            }

            _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
            _state = ScanState.Cooldown;
            return;
        }

        if (HasConfirmedPageReload()) {
            _state = ScanState.SyncQueue;
            return;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        if (!TryReadListingIdsHash(out var currentHash, out var nonZeroCount)) {
            _noMorePagesDetected = true;
            Plugin.Log.Debug($"DebugPfScanner: failed to read listings after next-page invoke (baseline_hash=0x{_pageReloadBaselineHash:X16}); treating as no more pages.");

            if (_detailCollector.PendingQueueCount == 0) {
                CompleteRun("no_more_pages_reload_unreadable");
                return;
            }

            _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
            _state = ScanState.Cooldown;
            return;
        }

        if (currentHash == _pageReloadBaselineHash) {
            _noMorePagesDetected = true;
            Plugin.Log.Debug($"DebugPfScanner: page did not change after next-page invoke (baseline_hash=0x{_pageReloadBaselineHash:X16}, non_zero={nonZeroCount}); treating as no more pages.");

            if (_detailCollector.PendingQueueCount == 0) {
                CompleteRun("no_more_pages_hash_unchanged");
                return;
            }

            _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
            _state = ScanState.Cooldown;
            return;
        }

        if (_latestVisible.Count == 0 && _incoming.IsEmpty) {
            if (!_pageReloadRefreshRequested) {
                _pageReloadRefreshRequested = true;
                TryRequestListingsUpdate(force: true);
                _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(GetPageReloadTimeoutMs());
                Plugin.Log.Debug($"DebugPfScanner: listing hash changed but no ReceiveListing updates arrived after next-page invoke; requested forced listings refresh (baseline_hash=0x{_pageReloadBaselineHash:X16}, current_hash=0x{currentHash:X16}, non_zero={nonZeroCount}).");
                return;
            }

            _noMorePagesDetected = true;
            Plugin.Log.Debug($"DebugPfScanner: listing hash changed but no ReceiveListing updates arrived after forced listings refresh (baseline_hash=0x{_pageReloadBaselineHash:X16}, current_hash=0x{currentHash:X16}, non_zero={nonZeroCount}); treating as no more pages.");

            if (_detailCollector.PendingQueueCount == 0) {
                CompleteRun("no_more_pages_no_receive_listing");
                return;
            }

            _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
            _state = ScanState.Cooldown;
            return;
        }

        Plugin.Log.Debug($"DebugPfScanner: listing hash changed after next-page invoke (baseline_hash=0x{_pageReloadBaselineHash:X16}, current_hash=0x{currentHash:X16}, non_zero={nonZeroCount}); resuming sync queue.");
        _pageReloadRefreshRequested = false;
        _state = ScanState.SyncQueue;
    }

    private void MarkAttempt(bool success, string reason) {
        if (_hasTarget) {
            _attemptedAt[_target.ListingId] = DateTime.UtcNow;
            Plugin.Log.Debug($"DebugPfScanner: listing={_target.ListingId} success={success} reason={reason}");
            _lastAttemptListingId = _target.ListingId;
        }

        _lastAttemptReason = reason;
        _lastAttemptSuccess = success;

        if (success) {
            _processedCount++;
            _consecutiveFailures = 0;
        } else {
            _consecutiveFailures++;
        }

        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
        _pageReloadBaselineHash = 0;
        _pageReloadRefreshRequested = false;
        _nextPagePreflightRequested = false;
        _noReadySinceUtc = DateTime.MinValue;
        _waitForQueueAckVersion = -1;
        _waitForAckVersion = -1;
        _waitForTerminalAckVersion = -1;
        _readyStableTicks = 0;
        _lastReadySnapshot = default;

        var cooldownMs = Math.Clamp(_plugin.Configuration.AutoDetailScanPostListingCooldownMs, 50, 3000);
        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(cooldownMs);
        _state = ScanState.Cooldown;
    }

    private void SetIdle() {
        _state = ScanState.Idle;
        _hasTarget = false;
        _retryTargetAfterCooldown = false;
        _openAttemptsForTarget = 0;
        _nextPagePreflightRequested = false;
    }

    private void CompleteRun(string reason) {
        Plugin.Log.Debug($"DebugPfScanner: complete reason={reason} processed={_processedCount} failures={_consecutiveFailures}");
        SetIdle();

        if (!_plugin.Configuration.EnableAutoDetailScanDebug) {
            return;
        }

        _plugin.Configuration.EnableAutoDetailScanDebug = false;
        _plugin.Configuration.Save();
    }

    private void DrainIncoming() {
        while (_incoming.TryDequeue(out var listing)) {
            UpsertVisibleCandidate(listing);
        }
    }

    private void PruneCaches() {
        var now = DateTime.UtcNow;

        // Keep visible cache fresh enough to represent the current PF page.
        var visibleTtl = TimeSpan.FromSeconds(30);
        foreach (var staleId in _latestVisible
                     .Where(kvp => now - kvp.Value.SeenAtUtc > visibleTtl)
                     .Select(kvp => kvp.Key)
                     .ToList()) {
            _latestVisible.Remove(staleId);
        }

        var dedupTtl = TimeSpan.FromSeconds(Math.Clamp(_plugin.Configuration.AutoDetailScanDedupTtlSeconds, 30, 3600));
        foreach (var staleId in _attemptedAt
                     .Where(kvp => now - kvp.Value > dedupTtl)
                     .Select(kvp => kvp.Key)
                     .ToList()) {
            _attemptedAt.Remove(staleId);
        }
    }

    private bool IsRecentlyAttempted(uint listingId) {
        if (!_attemptedAt.TryGetValue(listingId, out var lastAttemptUtc)) {
            return false;
        }

        var dedupTtl = TimeSpan.FromSeconds(Math.Clamp(_plugin.Configuration.AutoDetailScanDedupTtlSeconds, 30, 3600));
        return DateTime.UtcNow - lastAttemptUtc < dedupTtl;
    }

    private void RebuildPendingQueue() {
        _pending.Clear();
        foreach (var listing in _latestVisible.Values
                     .OrderBy(value => value.SeenAtUtc)
                     .ThenByDescending(value => value.BatchNumber)) {
            if (IsRecentlyAttempted(listing.ListingId)) {
                continue;
            }

            _pending.Enqueue(listing);
        }
    }

    private unsafe void TryRequestListingsUpdate(bool force = false) {
        var now = DateTime.UtcNow;
        if (!force && now < _nextRefreshAtUtc) {
            return;
        }

        var refreshMs = Math.Clamp(_plugin.Configuration.AutoDetailScanRefreshIntervalMs, 1000, 60000);
        _nextRefreshAtUtc = now.AddMilliseconds(refreshMs);

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return;
        }

        var ok = agent->RequestListingsUpdate();
        Plugin.Log.Debug($"DebugPfScanner: RequestListingsUpdate -> {ok}");
    }

    private unsafe PageAdvanceResult TryAdvanceToNextPage() {
        if (_plugin.Configuration.AutoDetailScanCurrentPageOnly) {
            return PageAdvanceResult.NoMorePages;
        }

        if (!TryReadListingIdsHash(out var baselineHash, out _)) {
            return PageAdvanceResult.NotReady;
        }

        var addon = _plugin.GameGui.GetAddonByName("LookingForGroup", 1);
        if (addon.IsNull || !addon.IsVisible || !addon.IsReady) {
            return PageAdvanceResult.NotReady;
        }

        var configuredFallbackButtonId = (uint)Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageButtonId, 0, 1000);
        var buttonId = ResolveNextPageButtonId(configuredFallbackButtonId);
        if (buttonId > 0) {
            if (!TryReadLookingForGroupButtonEnabled(buttonId, out var buttonEnabled)) {
                Plugin.Log.Debug($"DebugPfScanner: failed to read next-page button state (button_id={buttonId}).");
                return PageAdvanceResult.NotReady;
            }

            if (!buttonEnabled) {
                _nextPagePreflightRequested = false;
                Plugin.Log.Debug($"DebugPfScanner: next-page button disabled (button_id={buttonId}); treating as last page.");
                return PageAdvanceResult.NoMorePages;
            }

            if (!_nextPagePreflightRequested) {
                _nextPagePreflightRequested = true;
                TryRequestListingsUpdate(force: true);
                Plugin.Log.Debug($"DebugPfScanner: requested preflight listings refresh before next-page invoke (button_id={buttonId}).");
                return PageAdvanceResult.NotReady;
            }
        } else {
            _nextPagePreflightRequested = false;
        }

        var actionId = Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageActionId, 0, 1000);
        if (_capturedNextPageEvent.HasValue) {
            var replayResult = TryReplayNextPageEvent();
            if (replayResult == PageInvokeResult.Advanced) {
                Plugin.Log.Debug("DebugPfScanner: advanced using captured next-page event replay.");
                goto page_advanced;
            }
        }

        if (actionId > 0) {
            var agentResult = TrySendNextPageAgentEvent(actionId, buttonId <= 0);
            if (agentResult == PageInvokeResult.Advanced) {
                Plugin.Log.Debug($"DebugPfScanner: advanced using next-page agent event action={actionId}.");
                goto page_advanced;
            }
        }

        if (buttonId <= 0) {
            Plugin.Log.Debug($"DebugPfScanner: failed to advance page (capture={_capturedNextPageEvent.HasValue} action_id={actionId} button_id={buttonId}); disabled-state check unavailable until button id is known.");
            return PageAdvanceResult.NotReady;
        }

        if (configuredFallbackButtonId == 0 || buttonId != configuredFallbackButtonId) {
            Plugin.Log.Debug($"DebugPfScanner: skipped next-page button click fallback (resolved_button_id={buttonId} configured_fallback_button_id={configuredFallbackButtonId}); agent/replay path only.");
            return PageAdvanceResult.NotReady;
        }

        if (!TryReadLookingForGroupButtonEnabled(buttonId, out var clickButtonEnabled)) {
            Plugin.Log.Debug($"DebugPfScanner: failed to re-check next-page button before fallback click (button_id={buttonId}).");
            return PageAdvanceResult.NotReady;
        }

        if (!clickButtonEnabled) {
            _nextPagePreflightRequested = false;
            Plugin.Log.Debug($"DebugPfScanner: next-page button disabled before fallback click (button_id={buttonId}); treating as last page.");
            return PageAdvanceResult.NoMorePages;
        }

        if (TryClickLookingForGroupButton(buttonId)) {
            Plugin.Log.Debug($"DebugPfScanner: advanced using next-page button click (button_id={buttonId}).");
        } else {
            Plugin.Log.Debug($"DebugPfScanner: failed to advance page with fallback button click (button_id={buttonId}).");
            return PageAdvanceResult.NotReady;
        }

    page_advanced:
        _pageReloadBaselineHash = baselineHash;
        _pageReloadRefreshRequested = false;
        _nextPagePreflightRequested = false;

        _incoming.Clear();
        _latestVisible.Clear();
        _pending.Clear();
        _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(GetPageReloadTimeoutMs());
        _state = ScanState.WaitPageReload;
        return PageAdvanceResult.Advanced;
    }

    private uint ResolveNextPageButtonId(uint configuredButtonId) {
        if (configuredButtonId > 0) {
            return configuredButtonId;
        }

        if (_capturedNextPageButtonId > 0) {
            return (uint)_capturedNextPageButtonId;
        }

        var now = DateTime.UtcNow;
        if (now < _nextButtonResolveAtUtc) {
            return 0;
        }

        _nextButtonResolveAtUtc = now.AddMilliseconds(500);
        var actionId = Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageActionId, 0, 1000);
        if (actionId <= 0) {
            return 0;
        }

        if (!TryResolveNextPageButtonIdFromAddonEvents(actionId, out var resolvedButtonId, out var resolveDetails)) {
            return 0;
        }

        _capturedNextPageButtonId = (int)resolvedButtonId;
        _nextButtonResolveAtUtc = DateTime.MinValue;
        Plugin.Log.Debug($"DebugPfScanner: resolved next-page button id={resolvedButtonId} via node event mapping action={actionId} ({resolveDetails}).");
        return resolvedButtonId;
    }

    private unsafe bool TryResolveNextPageButtonIdFromAddonEvents(int actionId, out uint buttonId, out string resolveDetails) {
        buttonId = 0;
        resolveDetails = "no_candidates";

        var addon = _plugin.GameGui.GetAddonByName("LookingForGroup", 1);
        if (addon.IsNull || !addon.IsVisible || !addon.IsReady) {
            resolveDetails = "addon_not_ready";
            return false;
        }

        var addonPtr = (AtkUnitBase*)addon.Address;
        if (addonPtr->UldManager.NodeList == null || addonPtr->UldManager.NodeListCount == 0) {
            resolveDetails = "node_list_empty";
            return false;
        }

        var candidates = new List<uint>(4);
        var candidateLog = new StringBuilder();
        for (var index = 0; index < addonPtr->UldManager.NodeListCount; index++) {
            var node = addonPtr->UldManager.NodeList[index];
            if (node == null) {
                continue;
            }

            if (!TryGetNodeActionEventParam(node, (uint)actionId, out var matchedEventType)) {
                continue;
            }

            var nodeId = node->NodeId;
            if (nodeId == 0) {
                continue;
            }

            var button = addonPtr->GetComponentButtonById(nodeId);
            if (button == null) {
                continue;
            }

            candidates.Add(nodeId);
            if (candidateLog.Length > 0) {
                candidateLog.Append("; ");
            }

            candidateLog.Append($"id={nodeId} enabled={button->IsEnabled} evt={matchedEventType} x={node->ScreenX:0.#} y={node->ScreenY:0.#}");
        }

        if (candidates.Count == 0) {
            return false;
        }

        if (candidates.Count == 1) {
            buttonId = candidates[0];
            resolveDetails = candidateLog.ToString();
            return true;
        }

        buttonId = SelectLikelyNextButtonId(addonPtr, candidates);
        resolveDetails = $"{candidateLog}; selected={buttonId}";
        return true;
    }

    private static unsafe bool TryGetNodeActionEventParam(AtkResNode* node, uint actionId, out AtkEventType matchedEventType) {
        matchedEventType = 0;
        foreach (var eventType in NextButtonResolveEventTypes) {
            if (!node->IsEventRegistered(eventType)) {
                continue;
            }

            if (node->GetEventParam(eventType) != actionId) {
                continue;
            }

            matchedEventType = eventType;
            return true;
        }

        foreach (var eventType in Enum.GetValues<AtkEventType>()) {
            if (NextButtonResolveEventTypes.Contains(eventType)) {
                continue;
            }

            if (!node->IsEventRegistered(eventType)) {
                continue;
            }

            if (node->GetEventParam(eventType) != actionId) {
                continue;
            }

            matchedEventType = eventType;
            return true;
        }

        return false;
    }

    private static unsafe uint SelectLikelyNextButtonId(AtkUnitBase* addon, List<uint> candidates) {
        var selectedId = candidates[0];
        var bestX = float.MinValue;
        var bestY = float.MinValue;

        foreach (var candidate in candidates) {
            var button = addon->GetComponentButtonById(candidate);
            if (button == null || button->OwnerNode == null) {
                continue;
            }

            var node = button->OwnerNode;
            var x = node->ScreenX;
            var y = node->ScreenY;
            if (x > bestX || (Math.Abs(x - bestX) < 0.001f && y > bestY)) {
                bestX = x;
                bestY = y;
                selectedId = candidate;
            }
        }

        return selectedId;
    }

    private unsafe PageInvokeResult TrySendNextPageAgentEvent(int actionId, bool armButtonIdHydration) {
        if (actionId <= 0) {
            return PageInvokeResult.Failed;
        }

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return PageInvokeResult.Failed;
        }

        var values = stackalloc AtkValue[1];
        values[0].Type = AtkValueType.Int;
        values[0].Int = actionId;

        var returnValue = new AtkValue();
        _isReplayingNextPageEvent = true;
        try {
            if (armButtonIdHydration) {
                var autoHydrateUntilUtc = DateTime.UtcNow.Add(CaptureEventParamWindow);
                _autoHydrateNextPageButtonIdUntilUtc = autoHydrateUntilUtc;
                _autoCaptureNextPageEventUntilUtc = autoHydrateUntilUtc;
            }

            agent->ReceiveEvent(&returnValue, values, 1, 1);
            Plugin.Log.Debug($"DebugPfScanner: sent next-page agent event action={actionId} eventKind=1");
            return PageInvokeResult.Advanced;
        } catch (Exception ex) {
            Plugin.Log.Error($"DebugPfScanner: next-page agent event failed (action_id={actionId}): {ex.Message}");
            return PageInvokeResult.Failed;
        } finally {
            _isReplayingNextPageEvent = false;
            returnValue.ReleaseManagedMemory();
        }
    }

    private unsafe bool TryClickLookingForGroupButton(uint buttonId) {
        var addon = _plugin.GameGui.GetAddonByName("LookingForGroup", 1);
        if (addon.IsNull || !addon.IsVisible || !addon.IsReady) {
            return false;
        }

        var addonPtr = (AtkUnitBase*)addon.Address;
        var button = addonPtr->GetComponentButtonById(buttonId);
        if (button == null) {
            return false;
        }

        try {
            (*button).ClickAddonButton(addonPtr);
            return true;
        } catch (Exception ex) {
            Plugin.Log.Error($"DebugPfScanner: click next-page button failed: {ex.Message}");
            return false;
        }
    }

    private unsafe PageInvokeResult TryReplayNextPageEvent() {
        if (!_capturedNextPageEvent.HasValue) {
            return PageInvokeResult.Failed;
        }

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return PageInvokeResult.Failed;
        }

        var capture = _capturedNextPageEvent.Value;
        if (capture.Values.Length == 0) {
            return PageInvokeResult.Failed;
        }

        var replayValues = stackalloc AtkValue[capture.Values.Length];
        for (var i = 0; i < capture.Values.Length; i++) {
            replayValues[i].Type = capture.Values[i].Type;
            replayValues[i].UInt64 = capture.Values[i].Raw;
        }

        var returnValue = new AtkValue();
        _isReplayingNextPageEvent = true;
        try {
            if (capture.UsesWithResult) {
                agent->ReceiveEventWithResult(&returnValue, replayValues, (uint)capture.Values.Length, capture.EventKind);
            } else {
                agent->ReceiveEvent(&returnValue, replayValues, (uint)capture.Values.Length, capture.EventKind);
            }

            Plugin.Log.Debug($"DebugPfScanner: replayed next-page event kind={capture.EventKind} with_result={capture.UsesWithResult} values={capture.Values.Length} return={DescribeValue(returnValue.Type, returnValue.UInt64)}");
            return PageInvokeResult.Advanced;
        } catch (Exception ex) {
            Plugin.Log.Error($"DebugPfScanner: failed to replay next-page event: {ex.Message}");
            return PageInvokeResult.Failed;
        } finally {
            _isReplayingNextPageEvent = false;
            returnValue.ReleaseManagedMemory();
        }
    }

    private static unsafe bool TryCopyCaptureValues(AtkValue* values, uint valueCount, out CapturedAtkValue[] copiedValues, out string reason) {
        copiedValues = Array.Empty<CapturedAtkValue>();
        reason = "none";

        if (valueCount == 0 || valueCount > MaxCapturedAtkValues) {
            reason = "value_count_out_of_range";
            return false;
        }

        var copy = new CapturedAtkValue[valueCount];
        for (var index = 0; index < valueCount; index++) {
            var type = values[index].Type;
            if (!IsCaptureSafeType(type)) {
                reason = $"unsafe_type_{type}";
                return false;
            }

            copy[index] = new CapturedAtkValue(type, values[index].UInt64);
        }

        copiedValues = copy;
        return true;
    }

    private static bool IsCaptureSafeType(AtkValueType type) {
        if ((type & AtkValueType.Managed) != 0) {
            return false;
        }

        var baseType = type & AtkValueType.TypeMask;
        return baseType is AtkValueType.Null
            or AtkValueType.Bool
            or AtkValueType.Int
            or AtkValueType.Int64
            or AtkValueType.UInt
            or AtkValueType.UInt64
            or AtkValueType.Float;
    }

    private void TryHydrateCapturedNextPageButtonId(DateTime nowUtc) {
        if (!_capturedNextPageEvent.HasValue || _capturedNextPageButtonId > 0) {
            return;
        }

        if (_capturedNextPageEventAtUtc == DateTime.MinValue) {
            return;
        }

        if (nowUtc < _capturedNextPageEventAtUtc || nowUtc - _capturedNextPageEventAtUtc > CaptureEventParamWindow) {
            return;
        }

        var hydratedButtonId = ResolveCapturedNextPageButtonId();
        if (hydratedButtonId <= 0) {
            return;
        }

        _capturedNextPageButtonId = hydratedButtonId;
        Plugin.Log.Debug($"DebugPfScanner: hydrated captured next-page button id={hydratedButtonId} from addon receive-event stream.");
    }

    private int ResolveCapturedNextPageButtonId() {
        if (_recentAddonButtonEventParam > 0
            && DateTime.UtcNow - _recentAddonButtonEventAtUtc <= CaptureEventParamWindow) {
            return _recentAddonButtonEventParam;
        }

        return -1;
    }

    private unsafe bool TryReadPrimaryActionId(AtkValue* values, uint valueCount, out int actionId) {
        actionId = 0;
        if (valueCount == 0) {
            return false;
        }

        return TryConvertToActionId(values[0].Type, values[0].UInt64, out actionId);
    }

    private static bool TryReadPrimaryActionId(CapturedAtkValue[] values, out int actionId) {
        actionId = 0;
        if (values.Length == 0) {
            return false;
        }

        return TryConvertToActionId(values[0].Type, values[0].Raw, out actionId);
    }

    private static bool TryConvertToActionId(AtkValueType type, ulong raw, out int actionId) {
        actionId = 0;
        var baseType = type & AtkValueType.TypeMask;

        switch (baseType) {
            case AtkValueType.Int:
                actionId = unchecked((int)raw);
                return true;
            case AtkValueType.UInt:
                actionId = unchecked((int)(uint)raw);
                return true;
            case AtkValueType.Int64:
                if ((long)raw is < int.MinValue or > int.MaxValue) {
                    return false;
                }

                actionId = (int)(long)raw;
                return true;
            case AtkValueType.UInt64:
                if (raw > int.MaxValue) {
                    return false;
                }

                actionId = (int)raw;
                return true;
            default:
                return false;
        }
    }

    private unsafe bool TryReadLookingForGroupButtonEnabled(uint buttonId, out bool isEnabled) {
        isEnabled = false;

        var addon = _plugin.GameGui.GetAddonByName("LookingForGroup", 1);
        if (addon.IsNull || !addon.IsVisible || !addon.IsReady) {
            return false;
        }

        var addonPtr = (AtkUnitBase*)addon.Address;
        var button = addonPtr->GetComponentButtonById(buttonId);
        if (button == null) {
            return false;
        }

        isEnabled = button->IsEnabled;
        return true;
    }

    private unsafe string DescribeValues(AtkValue* values, uint valueCount) {
        var builder = new StringBuilder();
        var take = Math.Min((int)valueCount, 4);
        for (var index = 0; index < take; index++) {
            if (index > 0) {
                builder.Append(", ");
            }

            builder.Append(index);
            builder.Append(':');
            builder.Append(DescribeValue(values[index].Type, values[index].UInt64));
        }

        if (valueCount > 4) {
            builder.Append(", ...");
        }

        return builder.ToString();
    }

    private static string DescribeValues(CapturedAtkValue[] values) {
        var builder = new StringBuilder();
        var take = Math.Min(values.Length, 4);
        for (var index = 0; index < take; index++) {
            if (index > 0) {
                builder.Append(", ");
            }

            builder.Append(index);
            builder.Append(':');
            builder.Append(DescribeValue(values[index].Type, values[index].Raw));
        }

        if (values.Length > 4) {
            builder.Append(", ...");
        }

        return builder.ToString();
    }

    private static string DescribeValue(AtkValueType type, ulong raw) {
        var baseType = type & AtkValueType.TypeMask;
        return baseType switch {
            AtkValueType.Bool => $"{baseType}={(raw != 0)}",
            AtkValueType.Int => $"{baseType}={unchecked((int)raw)}",
            AtkValueType.Int64 => $"{baseType}={unchecked((long)raw)}",
            AtkValueType.UInt => $"{baseType}={unchecked((uint)raw)}",
            AtkValueType.UInt64 => $"{baseType}={raw}",
            AtkValueType.Float => $"{baseType}={BitConverter.Int32BitsToSingle(unchecked((int)raw)):0.###}",
            _ => $"{baseType}=0x{raw:X}",
        };
    }

    private unsafe bool HasConfirmedPageReload() {
        if (!TryReadListingIdsHash(out var currentHash, out var nonZeroCount)) {
            return false;
        }

        if (currentHash == _pageReloadBaselineHash) {
            return false;
        }

        if (nonZeroCount == 0) {
            return true;
        }

        return _latestVisible.Count > 0 || !_incoming.IsEmpty;
    }

    private unsafe bool TryReadListingIdsHash(out ulong hash, out int nonZeroCount) {
        hash = 0;
        nonZeroCount = 0;

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return false;
        }

        unchecked {
            var rollingHash = 1469598103934665603UL;
            for (var i = 0; i < 50; i++) {
                var listingId = agent->Listings.ListingIds[i];
                if (listingId != 0) {
                    nonZeroCount++;
                }

                rollingHash ^= listingId;
                rollingHash *= 1099511628211UL;
            }

            rollingHash ^= agent->NumberOfListingsDisplayed;
            rollingHash *= 1099511628211UL;
            hash = rollingHash;
        }

        return true;
    }

    private int GetPageReloadTimeoutMs() {
        var actionMs = Math.Clamp(_plugin.Configuration.AutoDetailScanActionIntervalMs, 100, 2000);
        var refreshMs = Math.Clamp(_plugin.Configuration.AutoDetailScanRefreshIntervalMs, 1000, 60000);
        var timeout = Math.Max(4000, Math.Min(12000, refreshMs + actionMs * 2));
        return timeout;
    }

    private void UpsertVisibleCandidate(ListingCandidate incoming) {
        if (_latestVisible.TryGetValue(incoming.ListingId, out var existing)) {
            _latestVisible[incoming.ListingId] = new ListingCandidate(
                incoming.ListingId,
                incoming.ContentId != 0 ? incoming.ContentId : existing.ContentId,
                existing.SeenAtUtc,
                Math.Max(existing.BatchNumber, incoming.BatchNumber)
            );
            return;
        }

        _latestVisible[incoming.ListingId] = incoming;
    }

    private int GetDetailReadyTimeoutMs() {
        return Math.Clamp(_plugin.Configuration.AutoDetailScanDetailTimeoutMs, 500, 10000);
    }

    private int GetDetailCollectionTimeoutMs(int minDwellMs) {
        var configuredTimeoutMs = GetDetailReadyTimeoutMs();
        var timeoutFloorFromDwellMs = minDwellMs + CollectionTimeoutExtraBudgetMs;
        return Math.Clamp(Math.Max(configuredTimeoutMs, timeoutFloorFromDwellMs), MinCollectionTimeoutMs, 10000);
    }

    private void StartRunFromFirstPage() {
        ResetSession();
        TryRequestListingsUpdate(force: true);

        var bootstrapWaitMs = Math.Clamp(_plugin.Configuration.AutoDetailScanActionIntervalMs, 100, 2000) + 200;
        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(bootstrapWaitMs);
        _state = ScanState.Cooldown;

        Plugin.Log.Debug($"DebugPfScanner: starting run from first page refresh (current_page_only={_plugin.Configuration.AutoDetailScanCurrentPageOnly})");
    }

    private bool IsLookingForGroupOpen() {
        return _plugin.GameGui.GetAddonByName("LookingForGroup", 1) != 0;
    }

    private unsafe DetailSnapshot GetCurrentDetailSnapshot() {
        if (_plugin.GameGui.GetAddonByName("LookingForGroupDetail", 1) == 0) {
            return default;
        }

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return default;
        }

        ref var detailed = ref agent->LastViewedListing;
        var effectiveParties = Math.Max(1, (int)detailed.NumberOfParties);
        var declaredSlots = Math.Max((int)detailed.TotalSlots, effectiveParties * 8);
        var totalSlots = Math.Clamp(declaredSlots, 0, 48);
        var nonZeroMembers = 0;

        for (var i = 0; i < totalSlots; i++) {
            if (detailed.MemberContentIds[i] != 0) {
                nonZeroMembers++;
            }
        }

        return new DetailSnapshot(
            detailed.ListingId,
            detailed.LeaderContentId,
            nonZeroMembers,
            totalSlots
        );
    }

    private readonly record struct CapturedAtkValue(AtkValueType Type, ulong Raw);
    private readonly record struct CapturedReceiveEvent(ulong EventKind, bool UsesWithResult, CapturedAtkValue[] Values);
    private readonly record struct DetailSnapshot(uint ListingId, ulong LeaderContentId, int NonZeroMembers, int TotalSlots);
    private readonly record struct ListingCandidate(uint ListingId, ulong ContentId, DateTime SeenAtUtc, int BatchNumber);

    private enum ScanState {
        Idle,
        SyncQueue,
        OpenTarget,
        WaitDetailReady,
        WaitCollected,
        WaitPageReload,
        Cooldown,
    }

    private enum PageAdvanceResult {
        Advanced,
        NoMorePages,
        NotReady,
    }

    private enum PageInvokeResult {
        Advanced,
        Failed,
    }
}
