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
    public async Task Background_worker_uses_base_delay_when_no_session_is_acquired()
    {
        var configuration = new Configuration
        {
            EnableFFLogsWorker = true,
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerBaseDelayMs = 4000,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 6, 30, 0, DateTimeKind.Utc),
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
                new FFLogsJobLeaseAttempt(null, false)),
            new FFLogsBatchProcessor(seams),
            new FFLogsResultSubmitter(seams, new FFLogsSubmitBuffer()),
            new FFLogsLeaseAbandoner(seams),
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { });

        await worker.RunAsync(CancellationToken.None);

        Assert.Equal([4000], delays);
    }

    [Fact]
    public async Task Background_worker_uses_base_delay_when_batch_processor_hits_rate_limit_cooldown()
    {
        var configuration = new Configuration
        {
            EnableFFLogsWorker = true,
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerBaseDelayMs = 3500,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var cooldownChecks = 0;
        var apiClient = new StubFFLogsApiClient
        {
            OnTryGetRateLimitRemaining = () =>
            {
                cooldownChecks++;
                return cooldownChecks >= 2
                    ? (true, TimeSpan.FromMinutes(3))
                    : (false, TimeSpan.Zero);
            },
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 6, 45, 0, DateTimeKind.Utc),
        };
        var seams = FFLogsCollector.CreateSeams(
            new StubFFLogsIngestHttpSender(),
            apiClient,
            timeProvider);
        var delays = new List<int>();
        var warnings = new List<string>();
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            warnings.Add,
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
                    new FFLogsLeaseSession(configuration.UploadUrls[0], [CreateJob()]),
                    false)),
            new FFLogsBatchProcessor(seams),
            new FFLogsResultSubmitter(seams, new FFLogsSubmitBuffer()),
            new FFLogsLeaseAbandoner(seams),
            static _ => { },
            warnings.Add,
            static _ => { },
            static _ => { });

        await worker.RunAsync(CancellationToken.None);

        Assert.Equal([3500], delays);
        Assert.Contains(warnings, message => message.Contains("FFLogs cooldown active", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Background_worker_applies_backoff_after_top_level_exception()
    {
        var configuration = new Configuration
        {
            EnableFFLogsWorker = true,
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerBaseDelayMs = 4500,
            FFLogsWorkerMaxBackoffDelayMs = 12000,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 7, 15, 0, DateTimeKind.Utc),
        };
        var seams = FFLogsCollector.CreateSeams(
            new StubFFLogsIngestHttpSender(),
            new StubFFLogsApiClient(),
            timeProvider);
        var delays = new List<int>();
        var errors = new List<string>();
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
            new ThrowingJobLeaseClient(seams),
            new FFLogsBatchProcessor(seams),
            new FFLogsResultSubmitter(seams, new FFLogsSubmitBuffer()),
            new FFLogsLeaseAbandoner(seams),
            static _ => { },
            static _ => { },
            errors.Add,
            static _ => { });

        await Assert.ThrowsAsync<TaskCanceledException>(() => worker.RunAsync(CancellationToken.None));

        Assert.Equal([4500], delays);
        Assert.Equal(4500, workerPolicy.LastBackoffDelayMs);
        Assert.Contains(errors, message => message.Contains("FFLogsCollector Loop Error", StringComparison.Ordinal));
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

    private static ParseJob CreateJob()
        => new()
        {
            ContentId = 1234,
            Name = "Example Player",
            Server = "TestServer",
            Region = "NA",
            ZoneId = 111,
            DifficultyId = 2,
            Partition = 1,
            EncounterId = 9001,
            LeaseToken = "lease-token",
        };

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

    private sealed class ThrowingJobLeaseClient(FFLogsCollectorSeams seams) : FFLogsJobLeaseClient(seams)
    {
        public override Task<FFLogsJobLeaseAttempt> TryAcquireSessionAsync(
            Configuration configuration,
            CancellationToken cancellationToken,
            Action<string>? warningLog = null,
            Action<string>? debugLog = null)
            => throw new InvalidOperationException("boom");
    }
}
