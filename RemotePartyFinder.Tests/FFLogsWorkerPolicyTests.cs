using System.Threading;
using System.Reflection;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class FFLogsWorkerPolicyTests {
    static FFLogsWorkerPolicyTests() {
        FFLogsTestAssemblyResolver.Register();
    }

    [Fact]
    public async Task Worker_policy_backoff_never_exceeds_configured_max() {
        var policy = new FFLogsWorkerPolicy(
            new Configuration {
                FFLogsWorkerBaseDelayMs = 5000,
                FFLogsWorkerMaxBackoffDelayMs = 12000,
                FFLogsWorkerJitterMs = 0,
            },
            _ => { },
            new ManualFFLogsTimeProvider { UtcNow = new DateTime(2026, 4, 14, 0, 0, 0, DateTimeKind.Utc) },
            static (_, _) => Task.CompletedTask);

        var nextFailures = await policy.DelayWithBackoffAsync(12, CancellationToken.None);

        Assert.Equal(13, nextFailures);
        Assert.Equal(12000, policy.LastBackoffDelayMs);
    }

    [Fact]
    public void Worker_policy_throttles_cooldown_skip_logging_to_once_per_minute() {
        var clock = new ManualFFLogsTimeProvider {
            UtcNow = new DateTime(2026, 4, 14, 1, 0, 0, DateTimeKind.Utc),
        };
        var warnings = new RecordingFFLogsWarningSink();
        var policy = new FFLogsWorkerPolicy(new Configuration(), warnings.Warning, clock);

        policy.LogCooldownSkipIfNeeded(TimeSpan.FromMinutes(5));

        clock.UtcNow = clock.UtcNow.AddSeconds(30);
        policy.LogCooldownSkipIfNeeded(TimeSpan.FromMinutes(4));

        clock.UtcNow = clock.UtcNow.AddSeconds(31);
        policy.LogCooldownSkipIfNeeded(TimeSpan.FromMinutes(3));

        Assert.Equal(2, warnings.Messages.Count);
        Assert.Contains("about 5 minute(s)", warnings.Messages[0], StringComparison.Ordinal);
        Assert.Contains("about 3 minute(s)", warnings.Messages[1], StringComparison.Ordinal);
    }

    [Fact]
    public void Collector_exposes_internal_dependency_seams_needed_by_future_fflogs_http_services() {
        var ingestHttpSender = new StubFFLogsIngestHttpSender();
        var apiClient = new StubFFLogsApiClient();
        var timeProvider = new ManualFFLogsTimeProvider {
            UtcNow = new DateTime(2026, 4, 14, 2, 0, 0, DateTimeKind.Utc),
        };

        var seams = FFLogsCollector.CreateSeams(ingestHttpSender, apiClient, timeProvider);

        Assert.Same(ingestHttpSender, seams.IngestHttpSender);
        Assert.Same(apiClient, seams.ApiClient);
        Assert.Same(timeProvider, seams.TimeProvider);
    }

    [Fact]
    public void Collector_factory_uses_injected_seams_submit_buffer_and_worker_policy_for_live_paths() {
        var configuration = new Configuration {
            FFLogsWorkerBaseDelayMs = 5000,
            FFLogsWorkerIdleDelayMs = 10000,
            FFLogsWorkerMaxBackoffDelayMs = 60000,
            FFLogsWorkerJitterMs = 0,
        };
        var timeProvider = new ManualFFLogsTimeProvider {
            UtcNow = new DateTime(2026, 4, 14, 3, 0, 0, DateTimeKind.Utc),
        };
        var warnings = new RecordingFFLogsWarningSink();
        var apiClient = new StubFFLogsApiClient {
            RateLimitCooldownUntilUtc = timeProvider.UtcNow.AddMinutes(5),
            OnTryGetRateLimitRemaining = static () => (true, TimeSpan.FromMinutes(5)),
        };
        var seams = FFLogsCollector.CreateSeams(new StubFFLogsIngestHttpSender(), apiClient, timeProvider);
        var submitBuffer = new FFLogsSubmitBuffer();
        submitBuffer.QueueSubmitResults([
            new ParseResult {
                ContentId = 7777,
                ZoneId = 88,
                DifficultyId = 5,
                Partition = 1,
                MatchedServer = "Injected",
            },
        ]);
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            warnings.Warning,
            timeProvider,
            static (_, _) => Task.CompletedTask);

        using var collector = FFLogsCollector.CreateForTesting(configuration, seams, submitBuffer, workerPolicy);

        Assert.Equal(apiClient.RateLimitCooldownUntilUtc, collector.RateLimitCooldownUntilUtc);
        Assert.True(collector.TryGetRateLimitCooldownRemaining(out var remaining));
        Assert.Equal(TimeSpan.FromMinutes(5), remaining);

        var buildSubmitBatch = typeof(FFLogsCollector).GetMethod(
            "BuildSubmitBatch",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(buildSubmitBatch);
        var batch = Assert.IsType<List<ParseResult>>(buildSubmitBatch!.Invoke(collector, new object[] { new List<ParseResult>() }));
        var queued = Assert.Single(batch);
        Assert.Equal(7777UL, queued.ContentId);
        Assert.Equal("Injected", queued.MatchedServer);

        var logCooldownSkipIfNeeded = typeof(FFLogsCollector).GetMethod(
            "LogCooldownSkipIfNeeded",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(logCooldownSkipIfNeeded);
        logCooldownSkipIfNeeded!.Invoke(collector, new object[] { TimeSpan.FromMinutes(5) });
        Assert.Single(warnings.Messages);
    }
}
