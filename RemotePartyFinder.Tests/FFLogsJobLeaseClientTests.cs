using System.Net;
using System.Net.Http;
using System.Text;
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
        Assert.Same(healthy, result.SelectedUploadUrl);
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

        Assert.Null(result.Session);
        Assert.Same(uploadUrl, result.SelectedUploadUrl);
        Assert.False(result.HadTransientFailure);
        Assert.True(uploadUrl.ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind.FflogsJobs));
    }
}
