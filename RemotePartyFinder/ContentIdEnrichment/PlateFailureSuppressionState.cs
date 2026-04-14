using System;
using System.Collections.Generic;

#nullable enable

namespace RemotePartyFinder;

internal enum SuppressionConsumption {
    None,
    WindowBudget,
    InFlightRequest,
}

internal sealed class PlateFailureSuppressionState {
    private static readonly TimeSpan DefaultGameLogSuppressionWindow = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan DefaultSelectOkSuppressionWindow = TimeSpan.FromSeconds(1);
    private const int DefaultGameLogSuppressionBudget = 32;
    private const int DefaultSelectOkSuppressionBudget = 8;
    private const int DefaultUiOpenBudget = 2;
    private const int DefaultSelectOkDialogBudget = 2;

    private readonly Dictionary<ulong, int> _suppressedUiOpenBudgets = [];
    private readonly Dictionary<ulong, int> _suppressedSelectOkDialogBudgets = [];
    private readonly int _gameLogSuppressionBudget;
    private readonly int _selectOkSuppressionBudget;
    private readonly TimeSpan _gameLogSuppressionWindow;
    private readonly TimeSpan _selectOkSuppressionWindow;
    private int _suppressedGameLogMessagesRemaining;
    private DateTime _suppressGameLogMessagesUntilUtc = DateTime.MinValue;
    private int _suppressedSelectOkEventsRemaining;
    private DateTime _suppressSelectOkUntilUtc = DateTime.MinValue;

    internal PlateFailureSuppressionState(
        int gameLogSuppressionBudget = DefaultGameLogSuppressionBudget,
        int selectOkSuppressionBudget = DefaultSelectOkSuppressionBudget,
        TimeSpan? gameLogSuppressionWindow = null,
        TimeSpan? selectOkSuppressionWindow = null
    ) {
        _gameLogSuppressionBudget = gameLogSuppressionBudget;
        _selectOkSuppressionBudget = selectOkSuppressionBudget;
        _gameLogSuppressionWindow = gameLogSuppressionWindow ?? DefaultGameLogSuppressionWindow;
        _selectOkSuppressionWindow = selectOkSuppressionWindow ?? DefaultSelectOkSuppressionWindow;
    }

    internal void TrackUiOpenBudget(ulong contentId, int budget = DefaultUiOpenBudget) {
        if (contentId == 0 || budget <= 0) {
            return;
        }

        _suppressedUiOpenBudgets[contentId] = budget;
    }

    internal void TrackSelectOkDialogBudget(ulong contentId, int budget = DefaultSelectOkDialogBudget) {
        if (contentId == 0 || budget <= 0) {
            return;
        }

        _suppressedSelectOkDialogBudgets[contentId] = budget;
    }

    internal void ArmDispatchedRequestUiSuppression(DateTime observedAtUtc) {
        _suppressedGameLogMessagesRemaining = _gameLogSuppressionBudget;
        _suppressGameLogMessagesUntilUtc = observedAtUtc.Add(_gameLogSuppressionWindow);
    }

    internal void ArmFailureSuppression(DateTime observedAtUtc) {
        ArmDispatchedRequestUiSuppression(observedAtUtc);
        _suppressedSelectOkEventsRemaining = _selectOkSuppressionBudget;
        _suppressSelectOkUntilUtc = observedAtUtc.Add(_selectOkSuppressionWindow);
    }

    internal bool TryConsumeSuppressedUiOpen(ulong contentId) {
        return TryConsumeBudget(_suppressedUiOpenBudgets, contentId);
    }

    internal bool TryConsumeSuppressedSelectOkDialog(ulong contentId) {
        return TryConsumeBudget(_suppressedSelectOkDialogBudgets, contentId);
    }

    internal SuppressionConsumption GetGameUiSuppression(DateTime observedAtUtc, bool hasInFlightRequest) {
        return ConsumeWindowBudget(
            observedAtUtc,
            ref _suppressedGameLogMessagesRemaining,
            _suppressGameLogMessagesUntilUtc,
            hasInFlightRequest
        );
    }

    internal SuppressionConsumption GetSelectOkSuppression(DateTime observedAtUtc, bool hasInFlightRequest) {
        return ConsumeWindowBudget(
            observedAtUtc,
            ref _suppressedSelectOkEventsRemaining,
            _suppressSelectOkUntilUtc,
            hasInFlightRequest
        );
    }

    private static bool TryConsumeBudget(Dictionary<ulong, int> budgets, ulong contentId) {
        if (!budgets.TryGetValue(contentId, out var remaining) || remaining <= 0) {
            return false;
        }

        if (remaining == 1) {
            budgets.Remove(contentId);
            return true;
        }

        budgets[contentId] = remaining - 1;
        return true;
    }

    private static SuppressionConsumption ConsumeWindowBudget(
        DateTime observedAtUtc,
        ref int remainingBudget,
        DateTime suppressUntilUtc,
        bool hasInFlightRequest
    ) {
        if (observedAtUtc > suppressUntilUtc) {
            remainingBudget = 0;
        }

        if (remainingBudget > 0) {
            remainingBudget--;
            return SuppressionConsumption.WindowBudget;
        }

        return hasInFlightRequest
            ? SuppressionConsumption.InFlightRequest
            : SuppressionConsumption.None;
    }
}
