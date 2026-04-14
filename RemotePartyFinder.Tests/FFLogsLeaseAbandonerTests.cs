using System.Collections.Immutable;
using System.Net;
using System.Net.Http;
using Newtonsoft.Json;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class FFLogsLeaseAbandonerTests
{
    static FFLogsLeaseAbandonerTests()
    {
        FFLogsTestAssemblyResolver.Register();
    }

    [Fact]
    public async Task Lease_abandoner_excludes_jobs_that_already_have_processed_results()
    {
        var configuration = CreateConfiguration();
        string payload = string.Empty;
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = async (request, _) =>
            {
                payload = await request.Content!.ReadAsStringAsync();
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{\"submitted\":1,\"released\":1,\"rejected\":0}"),
                };
            },
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), new ManualFFLogsTimeProvider());
        var abandoner = new FFLogsLeaseAbandoner(seams);
        var uploadUrl = new UploadUrl("http://127.0.0.1:8000");

        await abandoner.TryAbandonUnprocessedLeasesAsync(
            configuration,
            uploadUrl,
            [
                CreateJob(contentId: 1001, leaseToken: "lease-a"),
                CreateJob(contentId: 1002, leaseToken: "lease-b"),
            ],
            [
                CreateResult(contentId: 1001),
            ],
            "batch_failed",
            CancellationToken.None,
            static _ => { },
            static _ => { });

        Assert.Single(sender.Requests);
        var leases = JsonConvert.DeserializeObject<List<AbandonFflogsLease>>(payload);
        var releasedLease = Assert.Single(leases!);
        Assert.Equal(1002UL, releasedLease.ContentId);
        Assert.Equal("lease-b", releasedLease.LeaseToken);
    }

    [Fact]
    public async Task Lease_abandoner_invalidates_capability_on_auth_failure()
    {
        var configuration = CreateConfiguration();
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = static (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Forbidden)
            {
                Content = new StringContent("forbidden"),
            }),
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), new ManualFFLogsTimeProvider());
        var abandoner = new FFLogsLeaseAbandoner(seams);
        var uploadUrl = CreateUploadUrlWithAbandonCapability();

        await abandoner.TryAbandonUnprocessedLeasesAsync(
            configuration,
            uploadUrl,
            [CreateJob(contentId: 2001, leaseToken: "lease-a")],
            [],
            "auth_failed",
            CancellationToken.None,
            static _ => { },
            static _ => { });

        Assert.True(uploadUrl.ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind.FflogsLeasesAbandon));
        Assert.False(uploadUrl.TryGetProtectedEndpointCapability(ProtectedEndpointCapabilityKind.FflogsLeasesAbandon, out _));
    }

    [Fact]
    public async Task Lease_abandoner_treats_not_found_as_feature_unavailable_without_throwing()
    {
        var configuration = CreateConfiguration();
        var sender = new StubFFLogsIngestHttpSender
        {
            OnSendAsync = static (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new StringContent("missing"),
            }),
        };
        var seams = FFLogsCollector.CreateSeams(sender, new StubFFLogsApiClient(), new ManualFFLogsTimeProvider());
        var abandoner = new FFLogsLeaseAbandoner(seams);
        var uploadUrl = new UploadUrl("http://127.0.0.1:8000");
        var debugMessages = new List<string>();

        await abandoner.TryAbandonUnprocessedLeasesAsync(
            configuration,
            uploadUrl,
            [CreateJob(contentId: 3001, leaseToken: "lease-a")],
            [],
            "not_found",
            CancellationToken.None,
            static _ => { },
            debugMessages.Add);

        Assert.Single(sender.Requests);
        Assert.Contains(debugMessages, message => message.Contains("unavailable on this server version", StringComparison.Ordinal));
    }

    private static Configuration CreateConfiguration()
        => new()
        {
            IngestClientId = Guid.NewGuid().ToString("N"),
            UploadUrls = ImmutableList.Create(new UploadUrl("http://127.0.0.1:8000")),
        };

    private static UploadUrl CreateUploadUrlWithAbandonCapability()
    {
        var uploadUrl = new UploadUrl("http://127.0.0.1:8000");
        uploadUrl.ApplyIngestCapabilities(
            new ProtectedEndpointCapabilities
            {
                FflogsLeasesAbandon = new ProtectedEndpointCapabilityGrant
                {
                    Token = "lease-capability",
                    ExpiresAt = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds(),
                },
            },
            null);
        return uploadUrl;
    }

    private static ParseJob CreateJob(ulong contentId, string leaseToken)
        => new()
        {
            ContentId = contentId,
            ZoneId = 88,
            DifficultyId = 5,
            Partition = 2,
            LeaseToken = leaseToken,
        };

    private static ParseResult CreateResult(ulong contentId)
        => new()
        {
            ContentId = contentId,
            ZoneId = 88,
            DifficultyId = 5,
            Partition = 2,
            LeaseToken = "processed",
        };
}
