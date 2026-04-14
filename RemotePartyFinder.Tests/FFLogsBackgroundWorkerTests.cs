using System.Collections.Immutable;
using System.Net;
using System.Net.Http;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class FFLogsBackgroundWorkerTests
{
    static FFLogsBackgroundWorkerTests()
    {
        FFLogsTestAssemblyResolver.Register();
    }

    [Fact]
    public async Task Background_worker_uses_worker_policy_to_choose_idle_delay()
    {
        var configuration = new Configuration
        {
            EnableFFLogsWorker = true,
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerIdleDelayMs = 250,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 5, 0, 0, DateTimeKind.Utc),
        };
        var seams = FFLogsCollector.CreateSeams(
            new StubFFLogsIngestHttpSender(),
            new StubFFLogsApiClient(),
            timeProvider);
        var delays = new List<int>();
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            _ => { },
            timeProvider,
            (delayMs, _) =>
            {
                delays.Add(delayMs);
                throw new TaskCanceledException();
            });
        var worker = new FFLogsBackgroundWorker(
            configuration,
            seams.ApiClient,
            workerPolicy,
            new FixedAttemptJobLeaseClient(
                seams,
                new FFLogsJobLeaseAttempt(
                    new FFLogsLeaseSession(configuration.UploadUrls[0], []),
                    false)),
            new FFLogsBatchProcessor(seams),
            new FFLogsResultSubmitter(seams, new FFLogsSubmitBuffer()),
            new FFLogsLeaseAbandoner(seams),
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { });

        await worker.RunAsync(CancellationToken.None);

        Assert.Equal([1000], delays);
    }

    [Fact]
    public async Task Background_worker_applies_backoff_after_transient_iteration_failure()
    {
        var configuration = new Configuration
        {
            EnableFFLogsWorker = true,
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerBaseDelayMs = 5000,
            FFLogsWorkerMaxBackoffDelayMs = 12000,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 6, 0, 0, DateTimeKind.Utc),
        };
        var seams = FFLogsCollector.CreateSeams(
            new StubFFLogsIngestHttpSender(),
            new StubFFLogsApiClient(),
            timeProvider);
        var delays = new List<int>();
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            _ => { },
            timeProvider,
            (delayMs, _) =>
            {
                delays.Add(delayMs);
                throw new TaskCanceledException();
            });
        var worker = new FFLogsBackgroundWorker(
            configuration,
            seams.ApiClient,
            workerPolicy,
            new FixedAttemptJobLeaseClient(
                seams,
                new FFLogsJobLeaseAttempt(
                    new FFLogsLeaseSession(configuration.UploadUrls[0], []),
                    true)),
            new FFLogsBatchProcessor(seams),
            new FFLogsResultSubmitter(seams, new FFLogsSubmitBuffer()),
            new FFLogsLeaseAbandoner(seams),
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { });

        await worker.RunAsync(CancellationToken.None);

        Assert.Equal([5000], delays);
        Assert.Equal(5000, workerPolicy.LastBackoffDelayMs);
    }

    [Fact]
    public void Collector_remains_the_plugin_facing_facade_after_worker_extraction()
    {
        var configuration = new Configuration
        {
            IngestClientId = Guid.NewGuid().ToString("N"),
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var apiClient = new StubFFLogsApiClient
        {
            RateLimitCooldownUntilUtc = new DateTime(2026, 4, 15, 7, 30, 0, DateTimeKind.Utc),
            OnTryGetRateLimitRemaining = static () => (true, TimeSpan.FromMinutes(9)),
        };
        var seams = FFLogsCollector.CreateSeams(
            new StubFFLogsIngestHttpSender
            {
                OnSendAsync = static (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            },
            apiClient,
            new ManualFFLogsTimeProvider
            {
                UtcNow = new DateTime(2026, 4, 15, 7, 0, 0, DateTimeKind.Utc),
            });
        var workerPolicy = new FFLogsWorkerPolicy(configuration, _ => { }, seams.TimeProvider);

        using var collector = FFLogsCollector.CreateForTesting(
            configuration,
            seams,
            new FFLogsSubmitBuffer(),
            workerPolicy);

        Assert.True(collector.TryGetRateLimitCooldownRemaining(out var remaining));
        Assert.Equal(TimeSpan.FromMinutes(9), remaining);
        Assert.Equal(apiClient.RateLimitCooldownUntilUtc, collector.RateLimitCooldownUntilUtc);

        collector.ResetRateLimitCooldown();

        Assert.Equal(DateTime.MinValue, apiClient.RateLimitCooldownUntilUtc);
    }

    private sealed class FixedAttemptJobLeaseClient(
        FFLogsCollectorSeams seams,
        FFLogsJobLeaseAttempt attempt) : FFLogsJobLeaseClient(seams)
    {
        public override Task<FFLogsJobLeaseAttempt> TryAcquireSessionAsync(
            Configuration configuration,
            CancellationToken cancellationToken,
            Action<string>? warningLog = null,
            Action<string>? debugLog = null)
            => Task.FromResult(attempt);
    }
}
