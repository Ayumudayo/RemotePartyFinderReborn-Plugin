using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text.Json;
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
    public void IdentityUploadPayload_serializes_content_name_world_and_source() {
        var observedAtUtc = new DateTime(2026, 4, 13, 9, 30, 0, DateTimeKind.Utc);
        var snapshot = new CharacterIdentitySnapshot(
            1001UL,
            "Serialize Player",
            74,
            "Tonberry",
            observedAtUtc
        );

        var payload = CharacterIdentityUploadPayload.FromSnapshot(snapshot, observedAtUtc, "chara_card");
        var json = JsonSerializer.Serialize(payload);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.Equal(1001UL, root.GetProperty("content_id").GetUInt64());
        Assert.Equal("Serialize Player", root.GetProperty("name").GetString());
        Assert.Equal((uint)74, root.GetProperty("home_world").GetUInt32());
        Assert.Equal("Tonberry", root.GetProperty("world_name").GetString());
        Assert.Equal("chara_card", root.GetProperty("source").GetString());
        Assert.Equal("2026-04-13T09:30:00Z", root.GetProperty("observed_at").GetString());
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
    public async Task Identity_upload_attempt_times_out_and_leaves_identity_pending() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var configuration = new Configuration {
            UploadUrls = [new UploadUrl("http://127.0.0.1:8000") { IsEnabled = true }],
        };
        var snapshot = new CharacterIdentitySnapshot(
            2002UL,
            "Timeout Player",
            21,
            "Ravana",
            new DateTime(2026, 4, 13, 10, 30, 0, DateTimeKind.Utc)
        );
        database.UpsertResolvedIdentity(snapshot);

        using var httpClient = new HttpClient(new DelayedResponseHandler(TimeSpan.FromMilliseconds(500))) {
            Timeout = Timeout.InfiniteTimeSpan,
        };
        using var resolver = new CharaCardResolver(
            database,
            runtime: new FakeCharaCardResolverRuntime { PreflightResult = new ResolverPreflightResult(false, "hook unavailable") },
            configuration: configuration,
            identityUploadHttpClient: httpClient,
            identityUploadAttemptTimeout: TimeSpan.FromMilliseconds(100)
        );

        var drainTask = resolver.DrainPendingIdentityUploadsOnceAsync(CancellationToken.None);
        var completedTask = await Task.WhenAny(drainTask, Task.Delay(300));

        Assert.Same(drainTask, completedTask);
        Assert.False(await drainTask);
        Assert.Equal([snapshot], database.TakePendingIdentityUploads(10));
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
    public async Task Pump_runs_identity_upload_worker_single_flight_until_current_attempt_completes() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var snapshot = new CharacterIdentitySnapshot(
            3003UL,
            "Pump Player",
            79,
            "Omega",
            new DateTime(2026, 4, 13, 11, 30, 0, DateTimeKind.Utc)
        );
        database.UpsertResolvedIdentity(snapshot);

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;

        using var resolver = new CharaCardResolver(
            database,
            runtime: new FakeCharaCardResolverRuntime { PreflightResult = new ResolverPreflightResult(false, "hook unavailable") },
            uploadResolvedIdentitiesAsync: async (payloads, cancellationToken) => {
                Interlocked.Increment(ref attempts);
                started.TrySetResult();
                return await release.Task.WaitAsync(cancellationToken);
            }
        );

        resolver.Pump();
        await started.Task;
        resolver.Pump();

        Assert.Equal(1, Volatile.Read(ref attempts));

        release.SetResult(true);
        await WaitUntilAsync(() => database.TakePendingIdentityUploads(10).Count == 0);
        Assert.Equal(1, Volatile.Read(ref attempts));
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

    private sealed class DelayedResponseHandler(TimeSpan delay) : HttpMessageHandler {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            await Task.Delay(delay, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) {
                Content = new StringContent("[]"),
            };
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

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 1500, int pollMs = 20) {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline) {
            if (condition()) {
                return;
            }

            await Task.Delay(pollMs);
        }

        Assert.True(condition(), "Condition was not met within timeout.");
    }
}
