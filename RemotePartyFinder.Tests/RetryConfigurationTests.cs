using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class RetryConfigurationTests {
    [Theory]
    [InlineData(-1, 0)]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(3, 3)]
    public void Party_detail_retry_count_is_normalized_to_non_negative_retry_budget(int configuredRetries, int expectedRetries) {
        Assert.Equal(expectedRetries, PartyDetailCollector.NormalizeRetryCount(configuredRetries));
    }

    [Theory]
    [InlineData(0, 1, false)]
    [InlineData(1, 1, true)]
    [InlineData(2, 1, true)]
    [InlineData(0, 0, true)]
    public void Party_detail_drop_check_uses_retry_count_not_total_attempt_count(int currentRetryCount, int configuredRetries, bool shouldDrop) {
        Assert.Equal(shouldDrop, PartyDetailCollector.ShouldDropPendingDetail(currentRetryCount, configuredRetries));
    }

    [Theory]
    [InlineData(1, 1, true)]
    [InlineData(2, 1, false)]
    [InlineData(1, 0, false)]
    [InlineData(3, 3, true)]
    public void Debug_scanner_retry_budget_allows_initial_try_plus_configured_retries(int attemptsMade, int configuredRetries, bool shouldRetry) {
        Assert.Equal(shouldRetry, DebugPfScanner.ShouldRetryTargetAfterFailure(attemptsMade, configuredRetries));
    }

    [Theory]
    [InlineData(-1, 1)]
    [InlineData(0, 1)]
    [InlineData(1, 2)]
    [InlineData(3, 4)]
    public void Chara_card_retry_count_maps_to_total_attempt_budget(int configuredRetries, int expectedAttempts) {
        Assert.Equal(expectedAttempts, CharaCardResolver.ResolveMaxAttemptsFromRetryCount(configuredRetries));
    }

    [Theory]
    [InlineData(0, false, true)]
    [InlineData(0, true, false)]
    [InlineData(1, false, false)]
    [InlineData(2, true, false)]
    public void Debug_scanner_completes_once_targets_are_queued_without_waiting_for_detail_upload_queue(
        int pendingTargets,
        bool hasIncomingListings,
        bool shouldComplete) {
        Assert.Equal(
            shouldComplete,
            DebugPfScanner.ShouldCompleteRunWhenNoReadyTarget(pendingTargets, hasIncomingListings));
    }
}
