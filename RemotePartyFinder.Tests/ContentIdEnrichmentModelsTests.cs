using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class ContentIdEnrichmentModelsTests {
    [Fact]
    public void Enqueue_skips_duplicate_content_ids_when_already_inflight() {
        var nowUtc = new DateTime(2026, 4, 12, 12, 0, 0, DateTimeKind.Utc);
        var queue = new ContentIdResolveQueue();

        Assert.True(queue.Enqueue(1001UL, nowUtc));
        Assert.True(queue.TryStartNext(nowUtc, out var request));
        Assert.Equal(1001UL, request.ContentId);
        Assert.Equal(ResolveState.InFlight, queue.GetState(1001UL));

        Assert.False(queue.Enqueue(1001UL, nowUtc.AddSeconds(1)));
    }

    [Fact]
    public void Build_identity_payload_includes_world_name_when_present() {
        var observedAtUtc = new DateTime(2026, 4, 12, 12, 30, 0, DateTimeKind.Utc);
        var snapshot = new CharacterIdentitySnapshot(
            2002UL,
            "Alpha Beta",
            74,
            "Tonberry",
            observedAtUtc
        );

        var payload = CharacterIdentityUploadPayload.FromSnapshot(snapshot, observedAtUtc);

        Assert.Equal(snapshot.ContentId, payload.ContentId);
        Assert.Equal(snapshot.Name, payload.Name);
        Assert.Equal(snapshot.HomeWorld, payload.HomeWorld);
        Assert.Equal("Tonberry", payload.WorldName);
        Assert.Equal("chara_card", payload.Source);
        Assert.Equal(observedAtUtc, payload.ObservedAtUtc);
    }

    [Fact]
    public void Mark_timeout_moves_request_to_transient_failure() {
        var nowUtc = new DateTime(2026, 4, 12, 13, 0, 0, DateTimeKind.Utc);
        var queue = new ContentIdResolveQueue();

        Assert.True(queue.Enqueue(3003UL, nowUtc));
        Assert.True(queue.TryStartNext(nowUtc, out _));

        queue.MarkTimeout(3003UL, nowUtc.AddSeconds(5));

        var request = Assert.Single(queue.Requests);
        Assert.Equal(ResolveState.FailedTransient, request.State);
        Assert.Equal(nowUtc.AddSeconds(15), request.NextEligibleAttemptAtUtc);
    }

    [Fact]
    public void Backoff_schedule_advances_10s_30s_60s_for_repeated_timeouts() {
        var baseUtc = new DateTime(2026, 4, 12, 14, 0, 0, DateTimeKind.Utc);
        var queue = new ContentIdResolveQueue();

        Assert.True(queue.Enqueue(4004UL, baseUtc));

        Assert.True(queue.TryStartNext(baseUtc, out _));
        queue.MarkTimeout(4004UL, baseUtc);
        Assert.Equal(baseUtc.AddSeconds(10), queue.GetRequest(4004UL).NextEligibleAttemptAtUtc);

        Assert.True(queue.TryStartNext(baseUtc.AddSeconds(10), out _));
        queue.MarkTimeout(4004UL, baseUtc.AddSeconds(10));
        Assert.Equal(baseUtc.AddSeconds(40), queue.GetRequest(4004UL).NextEligibleAttemptAtUtc);

        Assert.True(queue.TryStartNext(baseUtc.AddSeconds(40), out _));
        queue.MarkTimeout(4004UL, baseUtc.AddSeconds(40));
        Assert.Equal(baseUtc.AddSeconds(100), queue.GetRequest(4004UL).NextEligibleAttemptAtUtc);
    }

    [Fact]
    public void Fresh_resolved_identity_is_not_requeued_within_ttl() {
        var resolvedAtUtc = new DateTime(2026, 4, 12, 15, 0, 0, DateTimeKind.Utc);
        var queue = new ContentIdResolveQueue();
        var snapshot = new CharacterIdentitySnapshot(
            5005UL,
            "Gamma Delta",
            21,
            "Ravana",
            resolvedAtUtc
        );

        queue.MarkResolved(snapshot);

        Assert.False(queue.Enqueue(5005UL, resolvedAtUtc.AddHours(1)));
        Assert.Equal(ResolveState.Resolved, queue.GetState(5005UL));
    }
}
