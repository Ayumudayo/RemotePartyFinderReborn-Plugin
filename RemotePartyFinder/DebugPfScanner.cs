using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
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
    private int _pageReloadRefreshAttempts;
    private DateTime _noReadySinceUtc = DateTime.MinValue;
    private int _consecutiveFailures;
    private int _processedCount;
    private DateTime _nextActionAtUtc = DateTime.MinValue;
    private DateTime _nextRefreshAtUtc = DateTime.MinValue;
    private DateTime _stateDeadlineUtc = DateTime.MinValue;
    private ulong _pageReloadBaselineHash;
    private long _waitForQueueAckVersion = -1;
    private long _waitForAckVersion = -1;
    private long _waitForTerminalAckVersion = -1;
    private DetailSnapshot _lastReadySnapshot;
    private int _readyStableTicks;
    private string _lastAttemptReason = "none";
    private bool _lastAttemptSuccess;
    private uint _lastAttemptListingId;
    private bool _wasEnabled;
    private bool _captureNextPageEventArmed;
    private DateTime _captureArmedAtUtc = DateTime.MinValue;
    private bool _isReplayingNextPageEvent;
    private CapturedReceiveEvent? _capturedNextPageEvent;
    private string _lastObservedReceiveEvent = "none";

    internal DebugPfScanner(Plugin plugin, PartyDetailCollector detailCollector, Gatherer gatherer) {
        _plugin = plugin;
        _detailCollector = detailCollector;
        _gatherer = gatherer;

        _plugin.PartyFinderGui.ReceiveListing += OnListing;
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
    internal bool CapturedNextPageUsesWithResult => _capturedNextPageEvent?.UsesWithResult ?? false;
    internal string CapturedNextPageValues => _capturedNextPageEvent.HasValue ? DescribeValues(_capturedNextPageEvent.Value.Values) : "none";
    internal string LastObservedReceiveEvent => _lastObservedReceiveEvent;

    public void Dispose() {
        _plugin.Framework.Update -= OnUpdate;
        _plugin.PartyFinderGui.ReceiveListing -= OnListing;
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
        _pageReloadRefreshAttempts = 0;
        _noReadySinceUtc = DateTime.MinValue;
        _consecutiveFailures = 0;
        _processedCount = 0;
        _nextActionAtUtc = DateTime.MinValue;
        _nextRefreshAtUtc = DateTime.MinValue;
        _stateDeadlineUtc = DateTime.MinValue;
        _pageReloadBaselineHash = 0;
        _waitForQueueAckVersion = -1;
        _waitForAckVersion = -1;
        _waitForTerminalAckVersion = -1;
        _lastReadySnapshot = default;
        _readyStableTicks = 0;
        _lastAttemptReason = "none";
        _lastAttemptSuccess = false;
        _lastAttemptListingId = 0;
    }

    internal void ArmNextPageCapture() {
        _captureNextPageEventArmed = true;
        _captureArmedAtUtc = DateTime.UtcNow;
        Plugin.Log.Debug("DebugPfScanner: armed next-page event capture");
    }

    internal void ClearNextPageCapture() {
        _captureNextPageEventArmed = false;
        _captureArmedAtUtc = DateTime.MinValue;
        _capturedNextPageEvent = null;
        _lastObservedReceiveEvent = "none";
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

    private unsafe void OnLookingForGroupReceiveEvent(AgentEvent eventType, AgentArgs args) {
        if (!_captureNextPageEventArmed || _isReplayingNextPageEvent) {
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

        if (!TryReadPrimaryActionId(valuesPtr, receiveEventArgs.ValueCount, out var actionId)) {
            Plugin.Log.Debug($"DebugPfScanner: skipped next-page capture event kind={receiveEventArgs.EventKind} reason=no_primary_action");
            return;
        }

        var expectedActionId = Math.Clamp(_plugin.Configuration.AutoDetailScanNextPageActionId, 0, 1000);
        if (actionId != expectedActionId) {
            Plugin.Log.Debug($"DebugPfScanner: ignored capture event action={actionId} expected={expectedActionId} kind={receiveEventArgs.EventKind}");
            return;
        }

        if (!TryCopyCaptureValues(valuesPtr, receiveEventArgs.ValueCount, out var values, out var reason)) {
            Plugin.Log.Debug($"DebugPfScanner: skipped next-page capture event kind={receiveEventArgs.EventKind} reason={reason}");
            return;
        }

        var usesWithResult = eventType == AgentEvent.PostReceiveEventWithResult;
        _capturedNextPageEvent = new CapturedReceiveEvent(receiveEventArgs.EventKind, usesWithResult, values);
        _captureNextPageEventArmed = false;
        _captureArmedAtUtc = DateTime.MinValue;
        Plugin.Log.Warning($"DebugPfScanner: captured next-page event action={actionId} kind={receiveEventArgs.EventKind} with_result={usesWithResult} value_count={values.Length} values=[{DescribeValues(values)}]");
    }

    private void OnUpdate(IFramework framework) {
        if (_tickGate.ElapsedMilliseconds < 50) {
            return;
        }

        _tickGate.Restart();

        if (_captureNextPageEventArmed && DateTime.UtcNow - _captureArmedAtUtc > CaptureArmTimeout) {
            _captureNextPageEventArmed = false;
            _captureArmedAtUtc = DateTime.MinValue;
            Plugin.Log.Warning("DebugPfScanner: next-page capture timed out; arm again and click Next page manually.");
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

            if (_detailCollector.PendingQueueCount == 0) {
                CompleteRun("pending_drained");
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

        if (HasConfirmedPageReload()) {
            _state = ScanState.SyncQueue;
            return;
        }

        if (DateTime.UtcNow <= _stateDeadlineUtc) {
            return;
        }

        if (_pageReloadRefreshAttempts < 1) {
            _pageReloadRefreshAttempts++;
            _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(GetPageReloadTimeoutMs());
            return;
        }

        if (_detailCollector.PendingQueueCount == 0) {
            CompleteRun("page_reload_timeout");
            return;
        }

        _nextActionAtUtc = DateTime.UtcNow.AddMilliseconds(250);
        _state = ScanState.Cooldown;
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
        _pageReloadRefreshAttempts = 0;
        _pageReloadBaselineHash = 0;
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

        if (!TryReplayNextPageEvent()) {
            Plugin.Log.Warning("DebugPfScanner: no replayable next-page event captured; arm capture and click Next page manually once.");
            return PageAdvanceResult.NotReady;
        }

        _pageReloadBaselineHash = baselineHash;
        _pageReloadRefreshAttempts = 0;

        _incoming.Clear();
        _latestVisible.Clear();
        _pending.Clear();
        _stateDeadlineUtc = DateTime.UtcNow.AddMilliseconds(GetPageReloadTimeoutMs());
        _state = ScanState.WaitPageReload;
        return PageAdvanceResult.Advanced;
    }

    private unsafe bool TryReplayNextPageEvent() {
        if (!_capturedNextPageEvent.HasValue) {
            return false;
        }

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) {
            return false;
        }

        var capture = _capturedNextPageEvent.Value;
        if (capture.Values.Length == 0) {
            return false;
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

            Plugin.Log.Debug($"DebugPfScanner: replayed next-page event kind={capture.EventKind} with_result={capture.UsesWithResult} values={capture.Values.Length}");
            return true;
        } catch (Exception ex) {
            Plugin.Log.Error($"DebugPfScanner: failed to replay next-page event: {ex.Message}");
            return false;
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
}
