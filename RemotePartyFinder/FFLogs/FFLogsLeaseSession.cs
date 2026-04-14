using System;
using System.Collections.Generic;
using System.Linq;

namespace RemotePartyFinder;

internal sealed class FFLogsLeaseSession
{
    public FFLogsLeaseSession(
        UploadUrl uploadUrl,
        IEnumerable<ParseJob> jobs,
        bool useBaseDelayWhenNoWork = false)
    {
        UploadUrl = uploadUrl ?? throw new ArgumentNullException(nameof(uploadUrl));
        ArgumentNullException.ThrowIfNull(jobs);
        Jobs = jobs.ToArray();
        UseBaseDelayWhenNoWork = useBaseDelayWhenNoWork;
    }

    public UploadUrl UploadUrl { get; }

    public IReadOnlyList<ParseJob> Jobs { get; }

    public bool UseBaseDelayWhenNoWork { get; }

    public bool HasJobs
        => Jobs.Count > 0;

    public bool TryBuildEndpointUrl(string endpointPath, out string endpointUrl)
        => IngestEndpointResolver.TryBuildEndpointUrl(UploadUrl, endpointPath, out endpointUrl);

    public bool ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind kind)
        => UploadUrl.ShouldDeferProtectedEndpointRequest(kind);

    public bool TryGetProtectedEndpointCapability(ProtectedEndpointCapabilityKind kind, out string token)
        => UploadUrl.TryGetProtectedEndpointCapability(kind, out token);

    public void MarkProtectedEndpointCapabilitiesRequired()
        => UploadUrl.MarkProtectedEndpointCapabilitiesRequired();

    public void InvalidateProtectedEndpointCapability(ProtectedEndpointCapabilityKind kind)
        => UploadUrl.InvalidateProtectedEndpointCapability(kind);
}
