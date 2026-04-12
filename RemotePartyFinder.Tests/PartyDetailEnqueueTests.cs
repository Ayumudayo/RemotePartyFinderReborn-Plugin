using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class PartyDetailEnqueueTests {
    [Fact]
    public void BuildDetailEnqueueSet_includes_leader_and_nonzero_members() {
        var payload = new UploadablePartyDetail {
            LeaderContentId = 111UL,
            MemberContentIds = [0UL, 222UL, 333UL, 222UL, 0UL],
        };

        var contentIds = PartyDetailCollector.BuildContentIdsForResolve(payload);

        Assert.Equal(new HashSet<ulong> { 111UL, 222UL, 333UL }, contentIds.ToHashSet());
    }

    [Fact]
    public void DetailUpload_path_continues_when_enrichment_is_disabled() {
        var payload = new UploadablePartyDetail {
            LeaderContentId = 111UL,
            MemberContentIds = [0UL, 222UL],
        };

        List<ulong>? captured = null;
        var exception = Record.Exception(() => {
            var enqueued = PartyDetailCollector.TryEnqueueContentIdsForResolve(payload, contentIds => {
                captured = contentIds.ToList();
                throw new InvalidOperationException("enrichment disabled");
            });

            Assert.False(enqueued);
        });

        Assert.Null(exception);
        Assert.Equal([111UL, 222UL], captured);
    }
}
