using System.Collections.Immutable;
using System.Net;
using System.Net.Http;
using System.Threading;
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
    public void Worker_policy_computes_idle_delay_without_touching_http_or_fflogs_dependencies() {
        var policy = new FFLogsWorkerPolicy(
            new Configuration {
                FFLogsWorkerIdleDelayMs = 250,
                FFLogsWorkerJitterMs = 0,
            },
            _ => { },
            new ManualFFLogsTimeProvider { UtcNow = new DateTime(2026, 4, 14, 1, 30, 0, DateTimeKind.Utc) });

        var idleDelayMs = policy.ComputeIdleDelayMs();

        Assert.Equal(1000, idleDelayMs);
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
    public async Task Collector_worker_loop_uses_worker_policy_delay_for_idle_path() {
        var configuration = new Configuration {
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerIdleDelayMs = 250,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider {
            UtcNow = new DateTime(2026, 4, 14, 3, 0, 0, DateTimeKind.Utc),
        };
        var warnings = new RecordingFFLogsWarningSink();
        var delays = new List<int>();
        var sender = new StubFFLogsIngestHttpSender {
            OnSendAsync = static (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) {
                Content = new StringContent("[]"),
            }),
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            warnings.Warning,
            timeProvider,
            (delayMs, _) => {
                delays.Add(delayMs);
                throw new TaskCanceledException();
            });

        using var collector = FFLogsCollector.CreateForTesting(configuration, seams, new FFLogsSubmitBuffer(), workerPolicy);

        await collector.RunWorkerLoopForTestingAsync();

        Assert.Empty(warnings.Messages);
        Assert.Equal([1000], delays);
    }

    [Fact]
    public async Task Collector_worker_loop_uses_worker_policy_delay_for_backoff_path() {
        var configuration = new Configuration {
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerBaseDelayMs = 5000,
            FFLogsWorkerMaxBackoffDelayMs = 12000,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider {
            UtcNow = new DateTime(2026, 4, 14, 4, 0, 0, DateTimeKind.Utc),
        };
        var delays = new List<int>();
        var sender = new StubFFLogsIngestHttpSender {
            OnSendAsync = static (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError) {
                Content = new StringContent("jobs failed"),
            }),
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            _ => { },
            timeProvider,
            (delayMs, _) => {
                delays.Add(delayMs);
                throw new TaskCanceledException();
            });

        using var collector = FFLogsCollector.CreateForTesting(configuration, seams, new FFLogsSubmitBuffer(), workerPolicy);

        await collector.RunWorkerLoopForTestingAsync();

        Assert.Equal([5000], delays);
        Assert.Equal(5000, workerPolicy.LastBackoffDelayMs);
    }
}
