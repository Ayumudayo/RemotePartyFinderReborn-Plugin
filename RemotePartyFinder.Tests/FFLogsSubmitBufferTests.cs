using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class FFLogsSubmitBufferTests {
    static FFLogsSubmitBufferTests() {
        FFLogsTestAssemblyResolver.Register();
    }

    [Fact]
    public void Submit_buffer_deduplicates_by_parse_key_and_keeps_latest_result() {
        var buffer = new FFLogsSubmitBuffer();
        var first = CreateResult(contentId: 1001, zoneId: 88, difficultyId: 5, partition: 2, matchedServer: "Alpha");
        var second = CreateResult(contentId: 1001, zoneId: 88, difficultyId: 5, partition: 2, matchedServer: "Beta");
        second.IsEstimated = true;
        second.LeaseToken = "lease-b";

        buffer.QueueSubmitResults([first, second]);

        var batch = buffer.BuildSubmitBatch([]);
        var queued = Assert.Single(batch);
        Assert.Equal("Beta", queued.MatchedServer);
        Assert.True(queued.IsEstimated);
        Assert.Equal("lease-b", queued.LeaseToken);
    }

    [Fact]
    public void Submit_buffer_build_returns_all_pending_results_in_a_single_batch() {
        var buffer = new FFLogsSubmitBuffer();
        buffer.QueueSubmitResults([CreateResult(contentId: 4001, zoneId: 55, difficultyId: 2, partition: 7, matchedServer: "Ravana")]);

        var batch = buffer.BuildSubmitBatch([
            CreateResult(contentId: 4002, zoneId: 55, difficultyId: 2, partition: 7, matchedServer: "Bismarck"),
            CreateResult(contentId: 4003, zoneId: 55, difficultyId: 2, partition: 7, matchedServer: "Sephirot"),
        ]);

        Assert.Equal(3, batch.Count);
        Assert.Contains(batch, result => result.ContentId == 4001UL && result.MatchedServer == "Ravana");
        Assert.Contains(batch, result => result.ContentId == 4002UL && result.MatchedServer == "Bismarck");
        Assert.Contains(batch, result => result.ContentId == 4003UL && result.MatchedServer == "Sephirot");
        Assert.Empty(buffer.BuildSubmitBatch([]));
    }

    [Fact]
    public void Submit_buffer_build_clears_pending_results() {
        var buffer = new FFLogsSubmitBuffer();
        buffer.QueueSubmitResults([CreateResult(contentId: 2002, zoneId: 77, difficultyId: 3, partition: 1)]);

        var batch = buffer.BuildSubmitBatch([]);

        Assert.Single(batch);
        Assert.Empty(buffer.BuildSubmitBatch([]));
    }

    [Fact]
    public void Submit_buffer_requeue_restores_failed_batch() {
        var buffer = new FFLogsSubmitBuffer();
        var failedBatch = buffer.BuildSubmitBatch([
            CreateResult(contentId: 3003, zoneId: 66, difficultyId: 4, partition: 2, matchedServer: "Gamma"),
            CreateResult(contentId: 3004, zoneId: 66, difficultyId: 4, partition: 2, matchedServer: "Delta"),
        ]);

        buffer.RequeueSubmitBatch(failedBatch);

        var requeued = buffer.BuildSubmitBatch([]);
        Assert.Equal(2, requeued.Count);
        Assert.Contains(requeued, result => result.ContentId == 3003UL && result.MatchedServer == "Gamma");
        Assert.Contains(requeued, result => result.ContentId == 3004UL && result.MatchedServer == "Delta");
    }

    [Fact]
    public void Submit_buffer_parse_result_key_is_shared_for_collector_submit_identity() {
        var first = CreateResult(contentId: 5005, zoneId: 44, difficultyId: 6, partition: 3, matchedServer: "Alpha");
        first.LeaseToken = "lease-alpha";
        var second = CreateResult(contentId: 5005, zoneId: 44, difficultyId: 6, partition: 3, matchedServer: "Beta");
        second.IsHidden = true;
        second.IsEstimated = true;
        second.LeaseToken = "lease-beta";

        var firstKey = FFLogsSubmitBuffer.GetParseResultKey(first);
        var secondKey = FFLogsSubmitBuffer.GetParseResultKey(second);

        Assert.Equal(firstKey, secondKey);
        Assert.Equal("5005:44:6:3", firstKey);
    }

    private static ParseResult CreateResult(
        ulong contentId,
        uint zoneId,
        int difficultyId,
        int partition,
        string matchedServer = "Tonberry") {
        return new ParseResult {
            ContentId = contentId,
            ZoneId = zoneId,
            DifficultyId = difficultyId,
            Partition = partition,
            MatchedServer = matchedServer,
            LeaseToken = "lease-a",
        };
    }
}
