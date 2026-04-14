using System;
using System.Collections.Generic;
using System.Linq;

namespace RemotePartyFinder;

internal sealed class FFLogsLeaseSession
{
    public FFLogsLeaseSession(UploadUrl uploadUrl, IEnumerable<ParseJob> jobs)
    {
        UploadUrl = uploadUrl ?? throw new ArgumentNullException(nameof(uploadUrl));
        ArgumentNullException.ThrowIfNull(jobs);
        Jobs = jobs.ToArray();
    }

    public UploadUrl UploadUrl { get; }

    public IReadOnlyList<ParseJob> Jobs { get; }
}
