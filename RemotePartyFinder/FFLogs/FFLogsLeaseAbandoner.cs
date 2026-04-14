using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace RemotePartyFinder;

internal sealed class FFLogsLeaseAbandoner
{
    private readonly FFLogsCollectorSeams _seams;

    public FFLogsLeaseAbandoner(FFLogsCollectorSeams seams)
    {
        _seams = seams ?? throw new ArgumentNullException(nameof(seams));
    }

    public async Task TryAbandonUnprocessedLeasesAsync(
        Configuration configuration,
        FFLogsLeaseSession leaseSession,
        IEnumerable<ParseJob> leasedJobs,
        IEnumerable<ParseResult> processedResults,
        string reason,
        CancellationToken cancellationToken,
        Action<string> warningLog,
        Action<string> debugLog)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(leaseSession);
        ArgumentNullException.ThrowIfNull(leasedJobs);
        ArgumentNullException.ThrowIfNull(processedResults);
        ArgumentNullException.ThrowIfNull(reason);
        ArgumentNullException.ThrowIfNull(warningLog);
        ArgumentNullException.ThrowIfNull(debugLog);

        var abandonBatch = BuildAbandonLeaseBatch(leasedJobs, processedResults, reason);
        if (abandonBatch.Count == 0)
        {
            return;
        }

        if (!leaseSession.TryBuildEndpointUrl("/contribute/fflogs/leases/abandon", out var abandonUrl))
        {
            return;
        }

        if (leaseSession.ShouldDeferProtectedEndpointRequest(
            ProtectedEndpointCapabilityKind.FflogsLeasesAbandon))
        {
            return;
        }

        var jsonContent = JsonConvert.SerializeObject(abandonBatch);
        var capabilityToken = leaseSession.TryGetProtectedEndpointCapability(
            ProtectedEndpointCapabilityKind.FflogsLeasesAbandon,
            out var cachedCapability)
            ? cachedCapability
            : null;

        try
        {
            using var abandonRequest = IngestRequestFactory.CreatePostJsonRequest(
                configuration,
                abandonUrl,
                "/contribute/fflogs/leases/abandon",
                jsonContent,
                capabilityToken
            );
            var response = await _seams.IngestHttpSender.SendAsync(abandonRequest, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                if (TryParseLeaseAbandonResponse(responseBody, out var parsed))
                {
                    warningLog(
                        $"FFLogsCollector: released abandoned leases {parsed.Released}/{parsed.Submitted} (rejected={parsed.Rejected}).");
                }
                else
                {
                    warningLog(
                        $"FFLogsCollector: released abandoned leases request succeeded (submitted={abandonBatch.Count}).");
                }

                return;
            }

            if (response.StatusCode == System.Net.HttpStatusCode.Forbidden
                || response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                leaseSession.MarkProtectedEndpointCapabilitiesRequired();
                leaseSession.InvalidateProtectedEndpointCapability(
                    ProtectedEndpointCapabilityKind.FflogsLeasesAbandon);
            }

            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                debugLog(
                    "FFLogsCollector: lease abandon endpoint is unavailable on this server version.");
                return;
            }

            warningLog(
                $"FFLogsCollector: failed to release abandoned leases ({response.StatusCode}) body={responseBody}");
        }
        catch (Exception ex)
        {
            debugLog($"FFLogsCollector: lease abandon request error: {ex.Message}");
        }
    }

    private static string ParseJobKey(ParseJob job)
        => $"{job.ContentId}:{job.ZoneId}:{job.DifficultyId}:{job.Partition}";

    private static List<AbandonFflogsLease> BuildAbandonLeaseBatch(
        IEnumerable<ParseJob> leasedJobs,
        IEnumerable<ParseResult> processedResults,
        string reason)
    {
        var processedKeys = new HashSet<string>(
            processedResults.Select(FFLogsSubmitBuffer.GetParseResultKey),
            StringComparer.Ordinal);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var batch = new List<AbandonFflogsLease>();

        foreach (var job in leasedJobs)
        {
            if (string.IsNullOrWhiteSpace(job.LeaseToken))
            {
                continue;
            }

            var key = ParseJobKey(job);
            if (processedKeys.Contains(key) || !seen.Add(key))
            {
                continue;
            }

            batch.Add(new AbandonFflogsLease
            {
                ContentId = job.ContentId,
                ZoneId = job.ZoneId,
                DifficultyId = job.DifficultyId,
                Partition = job.Partition,
                LeaseToken = job.LeaseToken,
                Reason = reason,
            });
        }

        return batch;
    }

    private static bool TryParseLeaseAbandonResponse(string content, out ContributeFflogsLeaseAbandonResponse parsed)
    {
        parsed = null;
        if (string.IsNullOrWhiteSpace(content))
        {
            return false;
        }

        try
        {
            parsed = JsonConvert.DeserializeObject<ContributeFflogsLeaseAbandonResponse>(content);
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }
}
