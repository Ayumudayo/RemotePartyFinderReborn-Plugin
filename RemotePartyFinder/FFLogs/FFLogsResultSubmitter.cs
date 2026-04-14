using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace RemotePartyFinder;

internal sealed record FFLogsResultSubmitAttempt(
    bool HadTransientFailure,
    bool ShouldUseBaseDelayBeforeNextPoll);

internal sealed class FFLogsResultSubmitter
{
    private readonly FFLogsCollectorSeams _seams;
    private readonly FFLogsSubmitBuffer _submitBuffer;

    public FFLogsResultSubmitter(FFLogsCollectorSeams seams, FFLogsSubmitBuffer submitBuffer)
    {
        _seams = seams ?? throw new ArgumentNullException(nameof(seams));
        _submitBuffer = submitBuffer ?? throw new ArgumentNullException(nameof(submitBuffer));
    }

    public async Task<FFLogsResultSubmitAttempt> TrySubmitResultsAsync(
        Configuration configuration,
        FFLogsLeaseSession leaseSession,
        IEnumerable<ParseResult> freshResults,
        CancellationToken cancellationToken,
        Action<string> infoLog,
        Action<string> warningLog,
        Action<string> errorLog)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(leaseSession);
        ArgumentNullException.ThrowIfNull(freshResults);
        ArgumentNullException.ThrowIfNull(infoLog);
        ArgumentNullException.ThrowIfNull(warningLog);
        ArgumentNullException.ThrowIfNull(errorLog);

        var submitBatch = _submitBuffer.BuildSubmitBatch(freshResults.ToList());
        if (submitBatch.Count == 0)
        {
            return new FFLogsResultSubmitAttempt(false, false);
        }

        if (!leaseSession.TryBuildEndpointUrl("/contribute/fflogs/results", out var submitUrl))
        {
            _submitBuffer.RequeueSubmitBatch(submitBatch);
            return new FFLogsResultSubmitAttempt(false, true);
        }

        if (leaseSession.ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind.FflogsResults))
        {
            _submitBuffer.RequeueSubmitBatch(submitBatch);
            return new FFLogsResultSubmitAttempt(false, true);
        }

        var jsonContent = JsonConvert.SerializeObject(submitBatch);

        try
        {
            var submitCapability = leaseSession.TryGetProtectedEndpointCapability(
                ProtectedEndpointCapabilityKind.FflogsResults,
                out var cachedCapability)
                ? cachedCapability
                : null;
            using var submitRequest = IngestRequestFactory.CreatePostJsonRequest(
                configuration,
                submitUrl,
                "/contribute/fflogs/results",
                jsonContent,
                submitCapability
            );
            var submitResp = await _seams.IngestHttpSender.SendAsync(submitRequest, cancellationToken);
            var submitRespBody = await submitResp.Content.ReadAsStringAsync(cancellationToken);

            if (submitResp.IsSuccessStatusCode)
            {
                if (TryParseResultsSubmitResponse(submitRespBody, out var parsed))
                {
                    infoLog(
                        $"Uploaded parse results: updated={parsed.Updated}, accepted={parsed.Accepted}/{parsed.Submitted}, rejected={parsed.Rejected}, status={parsed.Status}.");
                }
                else
                {
                    infoLog($"Uploaded {submitBatch.Count} parse results.");
                }

                return new FFLogsResultSubmitAttempt(false, false);
            }

            _submitBuffer.RequeueSubmitBatch(submitBatch);
            var isAuthFailure = submitResp.StatusCode == System.Net.HttpStatusCode.Forbidden
                || submitResp.StatusCode == System.Net.HttpStatusCode.Unauthorized;
            if (isAuthFailure)
            {
                leaseSession.MarkProtectedEndpointCapabilitiesRequired();
                leaseSession.InvalidateProtectedEndpointCapability(
                    ProtectedEndpointCapabilityKind.FflogsResults);
            }

            var hadTransientFailure = !isAuthFailure;
            if ((int)submitResp.StatusCode == 429)
            {
                var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(submitResp);
                if (retryAfter.HasValue)
                {
                    warningLog($"FFLogsCollector: results endpoint rate limited, retry_after={retryAfter.Value}s");
                }
            }

            errorLog($"Failed to upload results: {submitResp.StatusCode} (requeued {submitBatch.Count}) body={submitRespBody}");
            return new FFLogsResultSubmitAttempt(hadTransientFailure, false);
        }
        catch (Exception ex)
        {
            _submitBuffer.RequeueSubmitBatch(submitBatch);
            errorLog($"Failed to upload results (exception): {ex.Message} (requeued {submitBatch.Count})");
            return new FFLogsResultSubmitAttempt(true, false);
        }
    }

    private static bool TryParseResultsSubmitResponse(string content, out ContributeFflogsResultsResponse parsed)
    {
        parsed = null;
        if (string.IsNullOrWhiteSpace(content))
        {
            return false;
        }

        try
        {
            parsed = JsonConvert.DeserializeObject<ContributeFflogsResultsResponse>(content);
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }
}
