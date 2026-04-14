using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace RemotePartyFinder;

internal sealed record FFLogsJobLeaseAttempt(
    FFLogsLeaseSession Session,
    UploadUrl SelectedUploadUrl,
    bool HadTransientFailure);

internal sealed class FFLogsJobLeaseClient
{
    private readonly FFLogsCollectorSeams _seams;

    public FFLogsJobLeaseClient(FFLogsCollectorSeams seams)
    {
        _seams = seams ?? throw new ArgumentNullException(nameof(seams));
    }

    public async Task<FFLogsJobLeaseAttempt> TryAcquireSessionAsync(
        Configuration configuration,
        CancellationToken cancellationToken,
        Action<string> warningLog = null,
        Action<string> debugLog = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        warningLog ??= static _ => { };
        debugLog ??= static _ => { };

        var uploadUrl = SelectUploadUrl(configuration);
        if (uploadUrl == null)
        {
            return new FFLogsJobLeaseAttempt(null, null, false);
        }

        if (!IngestEndpointResolver.TryBuildEndpointUrl(uploadUrl, "/contribute/fflogs/jobs", out var workUrl))
        {
            return new FFLogsJobLeaseAttempt(null, null, false);
        }

        if (uploadUrl.ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind.FflogsJobs))
        {
            return new FFLogsJobLeaseAttempt(null, null, false);
        }

        try
        {
            var workCapability = uploadUrl.TryGetProtectedEndpointCapability(
                ProtectedEndpointCapabilityKind.FflogsJobs,
                out var cachedCapability)
                ? cachedCapability
                : null;
            using var workRequest = IngestRequestFactory.CreateGetRequest(
                configuration,
                workUrl,
                "/contribute/fflogs/jobs",
                workCapability);
            var response = await _seams.IngestHttpSender.SendAsync(workRequest, cancellationToken);
            if (response.IsSuccessStatusCode)
            {
                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                var jobs = JsonConvert.DeserializeObject<List<ParseJob>>(json) ?? [];
                uploadUrl.FailureCount = 0;
                return new FFLogsJobLeaseAttempt(
                    new FFLogsLeaseSession(uploadUrl, jobs),
                    uploadUrl,
                    false);
            }

            if (response.StatusCode != System.Net.HttpStatusCode.NotFound)
            {
                var isAuthFailure = response.StatusCode == System.Net.HttpStatusCode.Forbidden
                    || response.StatusCode == System.Net.HttpStatusCode.Unauthorized;
                if (isAuthFailure)
                {
                    uploadUrl.MarkProtectedEndpointCapabilitiesRequired();
                    uploadUrl.InvalidateProtectedEndpointCapability(
                        ProtectedEndpointCapabilityKind.FflogsJobs);
                }
                else
                {
                    uploadUrl.FailureCount++;
                    uploadUrl.LastFailureTime = _seams.TimeProvider.UtcNow;
                }

                if ((int)response.StatusCode == 429)
                {
                    var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(response);
                    if (retryAfter.HasValue)
                    {
                        warningLog($"FFLogsCollector: jobs endpoint rate limited, retry_after={retryAfter.Value}s");
                    }
                }

                return new FFLogsJobLeaseAttempt(null, uploadUrl, !isAuthFailure);
            }

            return new FFLogsJobLeaseAttempt(null, uploadUrl, false);
        }
        catch (Exception ex)
        {
            uploadUrl.FailureCount++;
            uploadUrl.LastFailureTime = _seams.TimeProvider.UtcNow;
            debugLog($"Error requesting work: {ex.Message}");
            return new FFLogsJobLeaseAttempt(null, uploadUrl, true);
        }
    }

    private UploadUrl SelectUploadUrl(Configuration configuration)
    {
        foreach (var candidate in configuration.UploadUrls.Where(static uploadUrl => uploadUrl.IsEnabled))
        {
            if (IsCircuitOpen(configuration, candidate))
            {
                continue;
            }

            if (!IngestEndpointResolver.TryBuildEndpointUrl(candidate, string.Empty, out _))
            {
                continue;
            }

            return candidate;
        }

        return null;
    }

    private bool IsCircuitOpen(Configuration configuration, UploadUrl uploadUrl)
    {
        if (uploadUrl.FailureCount < configuration.CircuitBreakerFailureThreshold)
        {
            return false;
        }

        var elapsedSinceFailure = _seams.TimeProvider.UtcNow - uploadUrl.LastFailureTime;
        return elapsedSinceFailure.TotalMinutes < configuration.CircuitBreakerBreakDurationMinutes;
    }
}
