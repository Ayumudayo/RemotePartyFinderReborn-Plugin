using Microsoft.Data.Sqlite;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class CharaCardResolverRuntimeTests {
    [Fact]
    public void ResolverQueue_picks_only_uncached_ids() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);
        database.UpsertResolvedIdentity(new CharacterIdentitySnapshot(
            1001UL,
            "Cached Player",
            74,
            "Tonberry",
            new DateTime(2026, 4, 13, 0, 0, 0, DateTimeKind.Utc)
        ));

        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 79 ? "Omega" : null,
            () => new DateTime(2026, 4, 13, 1, 0, 0, DateTimeKind.Utc)
        );

        resolver.EnqueueMany([0UL, 1001UL, 2002UL, 2002UL]);
        resolver.Pump();

        Assert.Equal([2002UL], runtime.RequestedContentIds);
    }

    [Fact]
    public void ResolverQueue_marks_response_as_resolved() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 2, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([3003UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(3003UL, 74, "Resolved Player"));

        Assert.Equal(ResolveState.Resolved, resolver.GetResolveState(3003UL));
        Assert.True(database.TryGetIdentity(3003UL, out var snapshot));
        Assert.Equal(
            new CharacterIdentitySnapshot(3003UL, "Resolved Player", 74, "Tonberry", nowUtc),
            snapshot
        );
    }

    [Fact]
    public void Hook_failure_disables_enrichment_without_throwing() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var runtime = new FakeCharaCardResolverRuntime {
            ThrowOnInitialize = true,
        };

        CharaCardResolver? instance = null;
        var resolver = Record.Exception(() => instance = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => new DateTime(2026, 4, 13, 3, 0, 0, DateTimeKind.Utc)
        ));

        Assert.Null(resolver);
        using var resolverInstance = Assert.IsType<CharaCardResolver>(instance);

        resolverInstance.EnqueueMany([4004UL]);
        resolverInstance.Pump();

        Assert.False(resolverInstance.IsEnabled);
        Assert.Empty(runtime.RequestedContentIds);
    }

    [Fact]
    public void Packet_projection_extracts_content_name_and_world() {
        var observedAtUtc = new DateTime(2026, 4, 13, 4, 0, 0, DateTimeKind.Utc);

        var projected = CharaCardResolver.TryProjectIdentity(
            new CharaCardPacketModel(5005UL, 21, "Projection Test"),
            worldId => worldId == 21 ? "Ravana" : null,
            observedAtUtc,
            out var snapshot
        );

        Assert.True(projected);
        Assert.Equal(
            new CharacterIdentitySnapshot(5005UL, "Projection Test", 21, "Ravana", observedAtUtc),
            snapshot
        );
    }

    [Fact]
    public void Dispose_stops_future_pump_activity_and_releases_runtime_handles() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var runtime = new FakeCharaCardResolverRuntime();
        var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => new DateTime(2026, 4, 13, 5, 0, 0, DateTimeKind.Utc)
        );

        resolver.EnqueueMany([6006UL]);
        resolver.Dispose();
        resolver.Pump();

        Assert.True(runtime.IsDisposed);
        Assert.Empty(runtime.RequestedContentIds);
    }

    private sealed class FakeCharaCardResolverRuntime : ICharaCardResolverRuntime {
        private Action<CharaCardPacketModel>? _packetHandler;

        public List<ulong> RequestedContentIds { get; } = [];

        public ResolverPreflightResult PreflightResult { get; init; } = new(true, "Ready");

        public bool ThrowOnInitialize { get; init; }

        public bool IsDisposed { get; private set; }

        public ResolverPreflightResult CheckAvailability() => PreflightResult;

        public void Initialize(Action<CharaCardPacketModel> packetHandler) {
            if (ThrowOnInitialize) {
                throw new InvalidOperationException("boom");
            }

            _packetHandler = packetHandler;
        }

        public bool TryRequest(ulong contentId) {
            RequestedContentIds.Add(contentId);
            return true;
        }

        public void Deliver(CharaCardPacketModel packet) {
            _packetHandler?.Invoke(packet);
        }

        public void Dispose() {
            IsDisposed = true;
            _packetHandler = null;
        }
    }

    private sealed class TempPlayerCacheDatabase : IDisposable {
        private readonly string _directoryPath;

        public TempPlayerCacheDatabase() {
            _directoryPath = Path.Combine(Path.GetTempPath(), "RemotePartyFinder.Tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, "player_cache.db");
        }

        public string DatabasePath { get; }

        public void Dispose() {
            SqliteConnection.ClearAllPools();

            if (Directory.Exists(_directoryPath)) {
                Directory.Delete(_directoryPath, recursive: true);
            }
        }
    }
}
