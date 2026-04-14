using System.Net;
using System.Net.Http;
using System.Text;
using System.Collections.Immutable;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class FFLogsJobLeaseClientTests
{
    static FFLogsJobLeaseClientTests()
    {
        FFLogsTestAssemblyResolver.Register();
    }

    [Fact]
    public async Task Job_lease_client_skips_circuit_open_endpoints_when_selecting_a_session()
    {
        var timeProvider = new ManualFFLogsTimeProvider { UtcNow = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc) };
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = static (_, _) => Task.FromResult(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        "[{\"content_id\":1001,\"zone_id\":77,\"difficulty_id\":5,\"partition\":1,\"lease_token\":\"lease-a\",\"name\":\"Player One\",\"home_world_id\":1}]",
                        Encoding.UTF8,
                        "application/json"),
                })
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
        var leaseClient = new FFLogsJobLeaseClient(seams);

        var openCircuit = new UploadUrl("https://circuit-open.example/")
        {
            FailureCount = 3,
            LastFailureTime = timeProvider.UtcNow,
        };
        var healthy = new UploadUrl("https://healthy.example/");
        var configuration = new Configuration
        {
            CircuitBreakerFailureThreshold = 3,
            CircuitBreakerBreakDurationMinutes = 1,
            IngestClientId = Guid.NewGuid().ToString("N"),
            UploadUrls = [openCircuit, healthy],
        };

        var result = await leaseClient.TryAcquireSessionAsync(configuration, CancellationToken.None);

        Assert.NotNull(result.Session);
        Assert.Same(healthy, result.Session!.UploadUrl);
        Assert.False(result.HadTransientFailure);
        Assert.Single(sender.Requests);
        Assert.StartsWith("https://healthy.example/", sender.Requests[0].RequestUri!.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Job_lease_client_invalidates_jobs_capability_on_auth_failure()
    {
        var timeProvider = new ManualFFLogsTimeProvider { UtcNow = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc) };
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = static (_, _) => Task.FromResult(
                new HttpResponseMessage(HttpStatusCode.Unauthorized)
                {
                    Content = new StringContent(string.Empty, Encoding.UTF8, "application/json"),
                })
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
        var leaseClient = new FFLogsJobLeaseClient(seams);
        var uploadUrl = new UploadUrl("https://secure.example/");
        uploadUrl.ApplyIngestCapabilities(
            new ProtectedEndpointCapabilities
            {
                FflogsJobs = new ProtectedEndpointCapabilityGrant
                {
                    Token = "jobs-token",
                    ExpiresAt = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds(),
                },
            },
            null);
        var configuration = new Configuration
        {
            IngestClientId = Guid.NewGuid().ToString("N"),
            UploadUrls = [uploadUrl],
        };

        var result = await leaseClient.TryAcquireSessionAsync(configuration, CancellationToken.None);

        Assert.NotNull(result.Session);
        Assert.Same(uploadUrl, result.Session!.UploadUrl);
        Assert.Empty(result.Session.Jobs);
        Assert.False(result.HadTransientFailure);
        Assert.True(uploadUrl.ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind.FflogsJobs));
    }

    [Fact]
    public async Task Job_lease_client_defers_protected_jobs_endpoint_without_transient_failure()
    {
        var timeProvider = new ManualFFLogsTimeProvider { UtcNow = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc) };
        var sender = new StubFFLogsIngestHttpSender();
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
        var leaseClient = new FFLogsJobLeaseClient(seams);
        var uploadUrl = new UploadUrl("https://secure.example/");
        uploadUrl.MarkProtectedEndpointCapabilitiesRequired();
        var configuration = new Configuration
        {
            IngestClientId = Guid.NewGuid().ToString("N"),
            UploadUrls = [uploadUrl],
        };

        var result = await leaseClient.TryAcquireSessionAsync(configuration, CancellationToken.None);

        Assert.NotNull(result.Session);
        Assert.Same(uploadUrl, result.Session!.UploadUrl);
        Assert.Empty(result.Session.Jobs);
        Assert.True(result.Session.UseBaseDelayWhenNoWork);
        Assert.False(result.HadTransientFailure);
        Assert.Empty(sender.Requests);
    }

    [Fact]
    public async Task Job_lease_client_treats_not_found_jobs_endpoint_as_quiet_no_work()
    {
        var timeProvider = new ManualFFLogsTimeProvider { UtcNow = new DateTime(2026, 4, 15, 0, 0, 0, DateTimeKind.Utc) };
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = static (_, _) => Task.FromResult(
                new HttpResponseMessage(HttpStatusCode.NotFound)
                {
                    Content = new StringContent(string.Empty, Encoding.UTF8, "application/json"),
                })
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
        var leaseClient = new FFLogsJobLeaseClient(seams);
        var uploadUrl = new UploadUrl("https://old-server.example/");
        var configuration = new Configuration
        {
            IngestClientId = Guid.NewGuid().ToString("N"),
            UploadUrls = [uploadUrl],
        };

        var result = await leaseClient.TryAcquireSessionAsync(configuration, CancellationToken.None);

        Assert.NotNull(result.Session);
        Assert.Same(uploadUrl, result.Session!.UploadUrl);
        Assert.Empty(result.Session.Jobs);
        Assert.False(result.HadTransientFailure);
        Assert.Equal(0, uploadUrl.FailureCount);
    }

    [Fact]
    public async Task Collector_continues_results_submission_through_the_lease_session_owner()
    {
        var configuration = new Configuration
        {
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerIdleDelayMs = 250,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(
                new UploadUrl("http://127.0.0.1:8000"),
                new UploadUrl("http://127.0.0.1:9000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 1, 0, 0, DateTimeKind.Utc),
        };
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = static (_, _) => Task.FromResult(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{\"status\":\"ok\",\"submitted\":1,\"accepted\":1,\"updated\":0,\"rejected\":0}"),
                }),
        };
        var apiClient = new StubFFLogsApiClient
        {
            OnFetchCharacterCandidateDataBatchAsync = static (queries, _, _, _, _, _) =>
            {
                return Task.FromResult(new Dictionary<string, FFLogsClient.CharacterFetchedData>
                {
                    [queries[0].Key] = new FFLogsClient.CharacterFetchedData
                    {
                        Hidden = false,
                        Parses =
                        [
                            new FFLogsClient.CharacterEncounterParse
                            {
                                EncounterId = 88,
                                Percentile = 99.1,
                            },
                        ],
                    },
                });
            },
        };
        var seams = FFLogsCollector.CreateSeams(sender, apiClient, timeProvider);
        var workerPolicy = new FFLogsWorkerPolicy(
            configuration,
            _ => { },
            timeProvider,
            (delayMs, _) => throw new TaskCanceledException());
        var sessionOwner = configuration.UploadUrls[1];
        var fixedJobLeaseClient = new FixedSessionJobLeaseClient(
            seams,
            new FFLogsLeaseSession(sessionOwner, [CreateCollectorJob()]));

        using var collector = FFLogsCollector.CreateForTesting(
            configuration,
            seams,
            new FFLogsSubmitBuffer(),
            workerPolicy,
            fixedJobLeaseClient);

        await collector.RunWorkerLoopForTestingAsync();

        var resultsRequest = Assert.Single(sender.Requests);
        Assert.StartsWith("http://127.0.0.1:9000/contribute/fflogs/results", resultsRequest.RequestUri!.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Collector_uses_base_delay_for_quiet_jobs_endpoint_deferral()
    {
        var configuration = new Configuration
        {
            FFLogsClientId = "client-id",
            FFLogsClientSecret = "client-secret",
            IngestClientId = Guid.NewGuid().ToString("N"),
            FFLogsWorkerBaseDelayMs = 4321,
            FFLogsWorkerIdleDelayMs = 9999,
            FFLogsWorkerJitterMs = 0,
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };
        var timeProvider = new ManualFFLogsTimeProvider
        {
            UtcNow = new DateTime(2026, 4, 15, 2, 0, 0, DateTimeKind.Utc),
        };
        var sender = new StubFFLogsIngestHttpSender();
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), timeProvider);
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
        var deferredSession = new FFLogsLeaseSession(configuration.UploadUrls[0], [], useBaseDelayWhenNoWork: true);
        var leaseClient = new FixedSessionJobLeaseClient(seams, deferredSession);

        using var collector = FFLogsCollector.CreateForTesting(
            configuration,
            seams,
            new FFLogsSubmitBuffer(),
            workerPolicy,
            leaseClient);

        await collector.RunWorkerLoopForTestingAsync();

        Assert.Equal([4321], delays);
        Assert.Empty(sender.Requests);
    }

    private static ParseJob CreateCollectorJob()
    {
        return new ParseJob
        {
            ContentId = 7001,
            Name = "Collector Test",
            Server = "Alpha",
            Region = "JP",
            ZoneId = 77,
            DifficultyId = 5,
            Partition = 1,
            EncounterId = 88,
            LeaseToken = "lease-owner",
        };
    }

    private sealed class FixedSessionJobLeaseClient(
        FFLogsCollectorSeams seams,
        FFLogsLeaseSession session) : FFLogsJobLeaseClient(seams)
    {
        private bool _leased;

        public override Task<FFLogsJobLeaseAttempt> TryAcquireSessionAsync(
            Configuration configuration,
            CancellationToken cancellationToken,
            Action<string>? warningLog = null,
            Action<string>? debugLog = null)
        {
            if (_leased)
            {
                return Task.FromResult(new FFLogsJobLeaseAttempt(
                    new FFLogsLeaseSession(
                        session.UploadUrl,
                        [],
                        useBaseDelayWhenNoWork: session.UseBaseDelayWhenNoWork),
                    false));
            }

            _leased = true;
            return Task.FromResult(new FFLogsJobLeaseAttempt(session, false));
        }
    }
}
