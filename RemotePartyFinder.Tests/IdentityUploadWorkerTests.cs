using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class IdentityUploadWorkerTests {
    static IdentityUploadWorkerTests() {
        DalamudAssemblyResolver.Register();
    }

    [Fact]
    public async Task Pending_identity_upload_worker_builds_payload_from_snapshot() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var resolvedAtUtc = new DateTime(2026, 4, 13, 10, 0, 0, DateTimeKind.Utc);
        var snapshot = new CharacterIdentitySnapshot(1101UL, "Payload Player", 74, "Tonberry", resolvedAtUtc);
        database.UpsertResolvedIdentity(snapshot);

        IReadOnlyList<CharacterIdentityUploadPayload>? capturedPayloads = null;
        using var resolver = new CharaCardResolver(
            database,
            runtime: new FakeCharaCardResolverRuntime { PreflightResult = new ResolverPreflightResult(false, "hook unavailable") },
            uploadResolvedIdentitiesAsync: (payloads, cancellationToken) => {
                capturedPayloads = payloads;
                return Task.FromResult(true);
            }
        );

        await resolver.DrainPendingIdentityUploadsOnceAsync(CancellationToken.None);

        Assert.NotNull(capturedPayloads);
        var payload = Assert.Single(capturedPayloads!);
        Assert.Equal(snapshot.ContentId, payload.ContentId);
        Assert.Equal(snapshot.Name, payload.Name);
        Assert.Equal(snapshot.HomeWorld, payload.HomeWorld);
        Assert.Equal(snapshot.WorldName, payload.WorldName);
        Assert.Equal("chara_card", payload.Source);
        Assert.Equal(snapshot.LastResolvedAtUtc, payload.ObservedAtUtc);
    }

    [Fact]
    public async Task Failed_upload_keeps_identity_pending_for_retry() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var snapshot = new CharacterIdentitySnapshot(
            2202UL,
            "Retry Player",
            21,
            "Ravana",
            new DateTime(2026, 4, 13, 11, 0, 0, DateTimeKind.Utc)
        );
        database.UpsertResolvedIdentity(snapshot);

        var attempts = 0;
        using var resolver = new CharaCardResolver(
            database,
            runtime: new FakeCharaCardResolverRuntime { PreflightResult = new ResolverPreflightResult(false, "hook unavailable") },
            uploadResolvedIdentitiesAsync: (payloads, cancellationToken) => {
                attempts++;
                return Task.FromResult(false);
            }
        );

        await resolver.DrainPendingIdentityUploadsOnceAsync(CancellationToken.None);

        Assert.Equal(1, attempts);
        Assert.Equal([snapshot], database.TakePendingIdentityUploads(10));
    }

    [Fact]
    public async Task Successful_upload_clears_pending_identity_flag() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var snapshot = new CharacterIdentitySnapshot(
            3303UL,
            "Submitted Player",
            79,
            "Omega",
            new DateTime(2026, 4, 13, 12, 0, 0, DateTimeKind.Utc)
        );
        database.UpsertResolvedIdentity(snapshot);

        using var resolver = new CharaCardResolver(
            database,
            runtime: new FakeCharaCardResolverRuntime { PreflightResult = new ResolverPreflightResult(false, "hook unavailable") },
            uploadResolvedIdentitiesAsync: (payloads, cancellationToken) => Task.FromResult(true)
        );

        await resolver.DrainPendingIdentityUploadsOnceAsync(CancellationToken.None);

        Assert.Empty(database.TakePendingIdentityUploads(10));
        Assert.True(database.TryGetIdentity(snapshot.ContentId, out var stored));
        Assert.Equal(snapshot, stored);
    }

    private sealed class FakeCharaCardResolverRuntime : ICharaCardResolverRuntime {
        public ResolverPreflightResult PreflightResult { get; init; } = new(true, "Ready");

        public ResolverPreflightResult CheckAvailability() => PreflightResult;

        public void Initialize(Action<CharaCardPacketModel> packetHandler) {
        }

        public bool TryRequest(ulong contentId) => true;

        public void Dispose() {
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
