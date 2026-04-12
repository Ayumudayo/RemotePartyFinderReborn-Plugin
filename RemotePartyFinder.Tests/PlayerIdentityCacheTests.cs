using System.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class PlayerIdentityCacheTests {
    [Fact]
    public void ContentIdEnrichment_assembly_does_not_reference_newtonsoft_json() {
        var referencedAssemblies = typeof(PlayerLocalDatabase)
            .Assembly
            .GetReferencedAssemblies()
            .Select(static assemblyName => assemblyName.Name)
            .ToArray();

        Assert.DoesNotContain("Newtonsoft.Json", referencedAssemblies);
    }

    [Fact]
    public void UpsertResolvedIdentity_persists_name_world_and_marks_pending_upload() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var resolvedAtUtc = new DateTime(2026, 4, 12, 16, 0, 0, DateTimeKind.Utc);
        var snapshot = new CharacterIdentitySnapshot(101UL, "Alpha Beta", 74, "Tonberry", resolvedAtUtc);

        database.UpsertResolvedIdentity(snapshot);

        Assert.True(database.TryGetIdentity(snapshot.ContentId, out var stored));
        Assert.Equal(snapshot, stored);

        var pending = database.TakePendingIdentityUploads(10);
        Assert.Equal([snapshot], pending);
    }

    [Fact]
    public void UpsertPartialIdentityName_persists_partial_without_marking_pending_upload() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var updatedAtUtc = new DateTime(2026, 4, 12, 16, 15, 0, DateTimeKind.Utc);
        database.UpsertPartialIdentityName(151UL, "Partial Player", updatedAtUtc);

        Assert.False(database.TryGetIdentity(151UL, out _));
        Assert.True(database.TryGetPartialIdentity(151UL, out var partial));
        Assert.Equal(
            new PartialCharacterIdentitySnapshot(151UL, "Partial Player", null, null, updatedAtUtc),
            partial
        );
        Assert.Empty(database.TakePendingIdentityUploads(10));
    }

    [Fact]
    public void TryGetIdentity_returns_false_for_unknown_content_id() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var found = database.TryGetIdentity(404UL, out var snapshot);

        Assert.False(found);
        Assert.Null(snapshot);
    }

    [Fact]
    public void MarkIdentityUploaded_clears_pending_upload_flag() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var snapshot = new CharacterIdentitySnapshot(
            202UL,
            "Gamma Delta",
            21,
            "Ravana",
            new DateTime(2026, 4, 12, 16, 30, 0, DateTimeKind.Utc)
        );

        database.UpsertResolvedIdentity(snapshot);
        database.MarkIdentityUploadsSubmitted([snapshot.ContentId]);

        Assert.True(database.TryGetIdentity(snapshot.ContentId, out var stored));
        Assert.Equal(snapshot, stored);
        Assert.Empty(database.TakePendingIdentityUploads(10));
    }

    [Fact]
    public void UpsertResolvedIdentity_removes_existing_partial_identity() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        database.UpsertPartialIdentityName(
            212UL,
            "Partial Before Complete",
            new DateTime(2026, 4, 12, 16, 20, 0, DateTimeKind.Utc)
        );

        database.UpsertResolvedIdentity(new CharacterIdentitySnapshot(
            212UL,
            "Complete Player",
            21,
            "Ravana",
            new DateTime(2026, 4, 12, 16, 25, 0, DateTimeKind.Utc)
        ));

        Assert.False(database.TryGetPartialIdentity(212UL, out _));
        Assert.True(database.TryGetIdentity(212UL, out var stored));
        Assert.Equal("Complete Player", stored!.Name);
    }

    [Fact]
    public void Initialize_migrates_existing_player_cache_without_data_loss() {
        using var harness = new TempPlayerCacheDatabase();
        CreateLegacyPlayersSchema(harness.DatabasePath);

        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        Assert.Equal(1, database.PendingCount);

        var player = Assert.Single(database.TakePendingBatch());
        Assert.Equal(301UL, player.ContentId);
        Assert.Equal("Legacy Player", player.Name);
        Assert.Equal((ushort)63, player.HomeWorld);
        Assert.Equal((ushort)65, player.CurrentWorld);
        Assert.Equal(901UL, player.AccountId);

        Assert.False(database.TryGetIdentity(player.ContentId, out var identity));
        Assert.Null(identity);
    }

    [Fact]
    public void Initialize_handles_existing_complete_identity_table_idempotently() {
        using var harness = new TempPlayerCacheDatabase();
        CreateLegacyPlayersSchema(harness.DatabasePath);
        CreatePartialIdentitySchema(harness.DatabasePath);

        using (var firstInitialization = new PlayerLocalDatabase(harness.DatabasePath)) {
            Assert.True(firstInitialization.TryGetIdentity(401UL, out var migrated));
            Assert.Equal(
                new CharacterIdentitySnapshot(
                    401UL,
                    "Migrated Identity",
                    79,
                    "Omega",
                    new DateTime(2026, 4, 12, 17, 0, 0, DateTimeKind.Utc)
                ),
                migrated
            );
            Assert.Empty(firstInitialization.TakePendingIdentityUploads(10));
        }

        using var secondInitialization = new PlayerLocalDatabase(harness.DatabasePath);
        Assert.True(secondInitialization.TryGetIdentity(401UL, out var storedAgain));
        Assert.Equal("Migrated Identity", storedAgain!.Name);
        Assert.Equal("Omega", storedAgain.WorldName);
        Assert.Empty(secondInitialization.TakePendingIdentityUploads(10));
    }

    [Fact]
    public void Initialize_handles_partial_identity_table_idempotently() {
        using var harness = new TempPlayerCacheDatabase();
        CreateLegacyPlayersSchema(harness.DatabasePath);
        CreatePartialIdentityFallbackSchema(harness.DatabasePath);

        using (var firstInitialization = new PlayerLocalDatabase(harness.DatabasePath)) {
            Assert.True(firstInitialization.TryGetPartialIdentity(402UL, out var partial));
            Assert.Equal(
                new PartialCharacterIdentitySnapshot(
                    402UL,
                    "Partial Identity",
                    null,
                    null,
                    new DateTime(2026, 4, 12, 17, 5, 0, DateTimeKind.Utc)
                ),
                partial
            );
            Assert.False(firstInitialization.TryGetIdentity(402UL, out _));
        }

        using var secondInitialization = new PlayerLocalDatabase(harness.DatabasePath);
        Assert.True(secondInitialization.TryGetPartialIdentity(402UL, out var partialAgain));
        Assert.Equal("Partial Identity", partialAgain!.Name);
        Assert.Null(partialAgain.HomeWorld);
        Assert.Null(partialAgain.WorldName);
        Assert.False(secondInitialization.TryGetIdentity(402UL, out _));
    }

    private static void CreateLegacyPlayersSchema(string databasePath) {
        using var connection = OpenConnection(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE players (
                content_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                home_world INTEGER NOT NULL,
                current_world INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                payload_hash TEXT NOT NULL,
                dirty INTEGER NOT NULL DEFAULT 1,
                first_seen_utc TEXT NOT NULL,
                last_seen_utc TEXT NOT NULL,
                last_uploaded_utc TEXT
            );
            CREATE INDEX idx_players_dirty_last_seen ON players(dirty, last_seen_utc);
            INSERT INTO players (
                content_id,
                name,
                home_world,
                current_world,
                account_id,
                payload_hash,
                dirty,
                first_seen_utc,
                last_seen_utc,
                last_uploaded_utc
            ) VALUES (
                301,
                'Legacy Player',
                63,
                65,
                901,
                'legacy-hash',
                1,
                '2026-04-12T15:00:00.0000000Z',
                '2026-04-12T15:05:00.0000000Z',
                NULL
            );
            """;
        command.ExecuteNonQuery();
    }

    private static void CreatePartialIdentitySchema(string databasePath) {
        using var connection = OpenConnection(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE player_identities (
                content_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                home_world INTEGER NOT NULL,
                world_name TEXT NOT NULL,
                last_resolved_utc TEXT NOT NULL
            );
            INSERT INTO player_identities (
                content_id,
                name,
                home_world,
                world_name,
                last_resolved_utc
            ) VALUES (
                401,
                'Migrated Identity',
                79,
                'Omega',
                '2026-04-12T17:00:00.0000000Z'
            );
            """;
        command.ExecuteNonQuery();
    }

    private static void CreatePartialIdentityFallbackSchema(string databasePath) {
        using var connection = OpenConnection(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE player_identity_partials (
                content_id INTEGER PRIMARY KEY,
                name TEXT,
                home_world INTEGER,
                world_name TEXT,
                last_updated_utc TEXT NOT NULL
            );
            INSERT INTO player_identity_partials (
                content_id,
                name,
                home_world,
                world_name,
                last_updated_utc
            ) VALUES (
                402,
                'Partial Identity',
                NULL,
                NULL,
                '2026-04-12T17:05:00.0000000Z'
            );
            """;
        command.ExecuteNonQuery();
    }

    private static SqliteConnection OpenConnection(string databasePath) {
        var connection = new SqliteConnection($"Data Source={databasePath};Mode=ReadWriteCreate;Cache=Shared");
        connection.Open();
        return connection;
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
