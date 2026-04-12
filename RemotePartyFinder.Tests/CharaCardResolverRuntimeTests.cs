using Microsoft.Data.Sqlite;
using System.Reflection;
using System.Threading;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class CharaCardResolverRuntimeTests {
    static CharaCardResolverRuntimeTests() {
        DalamudAssemblyResolver.Register();
    }

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
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(3003UL));

        resolver.Pump();

        Assert.Equal(ResolveState.Resolved, resolver.GetResolveState(3003UL));
        Assert.True(database.TryGetIdentity(3003UL, out var snapshot));
        Assert.Equal(
            new CharacterIdentitySnapshot(3003UL, "Resolved Player", 74, "Tonberry", nowUtc),
            snapshot
        );
    }

    [Fact]
    public void Pump_logs_dispatched_and_persisted_identity_lifecycle_for_successful_resolution() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var debugLogs = new List<string>();
        var nowUtc = new DateTime(2026, 4, 13, 2, 30, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc,
            debugSink: debugLogs.Add
        );

        resolver.EnqueueMany([3131UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(3131UL, 74, "Lifecycle Player"));
        resolver.Pump();

        Assert.Contains(
            debugLogs,
            message => message.Contains("dispatched identity request", StringComparison.Ordinal)
                && message.Contains("contentId=3131", StringComparison.Ordinal)
        );
        Assert.Contains(
            debugLogs,
            message => message.Contains("persisted resolved identity", StringComparison.Ordinal)
                && message.Contains("contentId=3131", StringComparison.Ordinal)
                && message.Contains("Lifecycle Player", StringComparison.Ordinal)
                && message.Contains("homeWorld=74", StringComparison.Ordinal)
                && message.Contains("Tonberry", StringComparison.Ordinal)
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

    [Fact]
    public void Pump_logs_warning_when_runtime_request_cannot_be_dispatched() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var warnings = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime {
            TryRequestResult = false,
        };
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => new DateTime(2026, 4, 13, 5, 30, 0, DateTimeKind.Utc),
            warningSink: warnings.Add
        );

        resolver.EnqueueMany([6116UL]);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(6116UL));
        Assert.Contains(
            warnings,
            message => message.Contains("failed to dispatch identity request", StringComparison.Ordinal)
                && message.Contains("contentId=6116", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void ResolverQueue_times_out_accepted_request_and_retries_after_backoff_when_no_packet_arrives() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 6, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => nowUtc,
            requestTimeout: TimeSpan.FromSeconds(5)
        );

        resolver.EnqueueMany([7007UL]);
        resolver.Pump();

        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(7007UL));
        Assert.Equal([7007UL], runtime.RequestedContentIds);

        nowUtc = nowUtc.AddSeconds(6);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(7007UL));
        Assert.Equal([7007UL], runtime.RequestedContentIds);

        nowUtc = nowUtc.AddSeconds(10);
        resolver.Pump();

        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(7007UL));
        Assert.Equal([7007UL, 7007UL], runtime.RequestedContentIds);
    }

    [Fact]
    public void Packet_receipt_is_buffered_until_pump_persists_identity() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var persisted = new List<CharacterIdentitySnapshot>();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => new DateTime(2026, 4, 13, 7, 0, 0, DateTimeKind.Utc),
            persistResolvedIdentity: snapshot => persisted.Add(snapshot)
        );

        resolver.EnqueueMany([8008UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(8008UL, 74, "Buffered Player"));

        Assert.Empty(persisted);
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(8008UL));

        resolver.Pump();

        Assert.Equal(
            [
                new CharacterIdentitySnapshot(
                    8008UL,
                    "Buffered Player",
                    74,
                    "Tonberry",
                    new DateTime(2026, 4, 13, 7, 0, 0, DateTimeKind.Utc))
            ],
            persisted
        );
        Assert.Equal(ResolveState.Resolved, resolver.GetResolveState(8008UL));
    }

    [Fact]
    public void Persistence_failure_leaves_request_retryable_instead_of_resolved() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 8, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 21 ? "Ravana" : null,
            () => nowUtc,
            persistResolvedIdentity: _ => throw new InvalidOperationException("persist failed")
        );

        resolver.EnqueueMany([9009UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(9009UL, 21, "Retry Player"));

        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(9009UL));

        var exception = Record.Exception(() => resolver.Pump());

        Assert.Null(exception);
        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(9009UL));
        Assert.False(database.TryGetIdentity(9009UL, out _));

        nowUtc = nowUtc.AddSeconds(10);
        resolver.Pump();

        Assert.Equal([9009UL, 9009UL], runtime.RequestedContentIds);
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(9009UL));
    }

    [Fact]
    public void Unsolicited_packet_is_ignored_without_warning_or_exception() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var warnings = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => new DateTime(2026, 4, 13, 9, 0, 0, DateTimeKind.Utc),
            warningSink: warnings.Add
        );

        var exception = Record.Exception(() => runtime.Deliver(new CharaCardPacketModel(123456UL, 74, "Manual Player")));

        Assert.Null(exception);
        Assert.Empty(warnings);
        Assert.Equal(ResolveState.Unknown, resolver.GetResolveState(123456UL));
        Assert.False(database.TryGetIdentity(123456UL, out _));
    }

    private sealed class FakeCharaCardResolverRuntime : ICharaCardResolverRuntime {
        private Action<CharaCardPacketModel>? _packetHandler;

        public List<ulong> RequestedContentIds { get; } = [];

        public ResolverPreflightResult PreflightResult { get; init; } = new(true, "Ready");

        public bool ThrowOnInitialize { get; init; }

        public bool TryRequestResult { get; init; } = true;

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
            return TryRequestResult;
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

    private static class DalamudAssemblyResolver {
        private static int _registered;

        public static void Register() {
            if (Interlocked.Exchange(ref _registered, 1) != 0) {
                return;
            }

            AppDomain.CurrentDomain.AssemblyResolve += static (_, args) => {
                var assemblyName = new AssemblyName(args.Name).Name;
                if (string.IsNullOrWhiteSpace(assemblyName)) {
                    return null;
                }

                var dalamudHome = Environment.GetEnvironmentVariable("DALAMUD_HOME");
                if (string.IsNullOrWhiteSpace(dalamudHome)) {
                    dalamudHome = Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                        "XIVLauncher",
                        "addon",
                        "Hooks",
                        "dev"
                    );
                }

                var candidatePath = Path.Combine(dalamudHome, assemblyName + ".dll");
                return File.Exists(candidatePath) ? Assembly.LoadFrom(candidatePath) : null;
            };
        }
    }
}
