using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace RemotePartyFinder;

internal sealed class PlayerLocalDatabase : IDisposable {
    public const int MaxBatchSize = 100;

    private const int CurrentSchemaVersion = 2;
    private const string PlayersTableName = "players";
    private const string IdentitiesTableName = "player_identities";

    private readonly string _databasePath;
    private readonly object _sync = new();
    private bool _disposed;

    public PlayerLocalDatabase(string databasePath) {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        _databasePath = databasePath;

        var directoryPath = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrWhiteSpace(directoryPath)) {
            Directory.CreateDirectory(directoryPath);
        }

        Initialize();
    }

    public int PendingCount {
        get {
            lock (_sync) {
                using var connection = OpenConnection();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM players WHERE dirty = 1;";
                var value = command.ExecuteScalar();
                return value is long count ? (int)Math.Min(count, int.MaxValue) : 0;
            }
        }
    }

    public void UpsertObservedPlayers(IEnumerable<UploadablePlayer> players) {
        lock (_sync) {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO players (
                    content_id,
                    name,
                    home_world,
                    current_world,
                    account_id,
                    payload_hash,
                    dirty,
                    first_seen_utc,
                    last_seen_utc
                )
                VALUES (
                    @content_id,
                    @name,
                    @home_world,
                    @current_world,
                    @account_id,
                    @payload_hash,
                    1,
                    @observed_at,
                    @observed_at
                )
                ON CONFLICT(content_id) DO UPDATE SET
                    name = excluded.name,
                    home_world = excluded.home_world,
                    current_world = excluded.current_world,
                    account_id = excluded.account_id,
                    last_seen_utc = excluded.last_seen_utc,
                    payload_hash = excluded.payload_hash,
                    dirty = CASE
                        WHEN players.payload_hash <> excluded.payload_hash THEN 1
                        ELSE players.dirty
                    END;
                """;

            var contentIdParam = command.Parameters.Add("@content_id", SqliteType.Integer);
            var nameParam = command.Parameters.Add("@name", SqliteType.Text);
            var homeWorldParam = command.Parameters.Add("@home_world", SqliteType.Integer);
            var currentWorldParam = command.Parameters.Add("@current_world", SqliteType.Integer);
            var accountIdParam = command.Parameters.Add("@account_id", SqliteType.Integer);
            var payloadHashParam = command.Parameters.Add("@payload_hash", SqliteType.Text);
            var observedAtParam = command.Parameters.Add("@observed_at", SqliteType.Text);

            var observedAt = DateTimeOffset.UtcNow.ToString("O");
            observedAtParam.Value = observedAt;

            foreach (var player in players) {
                contentIdParam.Value = (long)player.ContentId;
                nameParam.Value = player.Name;
                homeWorldParam.Value = (long)player.HomeWorld;
                currentWorldParam.Value = (long)player.CurrentWorld;
                accountIdParam.Value = unchecked((long)player.AccountId);
                payloadHashParam.Value = ComputePayloadHash(player);
                command.ExecuteNonQuery();
            }

            PruneCleanRows(connection, transaction);
            transaction.Commit();
        }
    }

    public List<UploadablePlayer> TakePendingBatch(int maxBatchSize = MaxBatchSize) {
        lock (_sync) {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT content_id, name, home_world, current_world, account_id
                FROM players
                WHERE dirty = 1
                ORDER BY last_seen_utc DESC
                LIMIT @limit;
                """;
            command.Parameters.AddWithValue("@limit", Math.Clamp(maxBatchSize, 1, MaxBatchSize));

            var batch = new List<UploadablePlayer>();
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                batch.Add(new UploadablePlayer {
                    ContentId = (ulong)reader.GetInt64(0),
                    Name = reader.GetString(1),
                    HomeWorld = Convert.ToUInt16(reader.GetInt64(2)),
                    CurrentWorld = Convert.ToUInt16(reader.GetInt64(3)),
                    AccountId = unchecked((ulong)reader.GetInt64(4)),
                });
            }

            return batch;
        }
    }

    public void MarkBatchUploaded(IEnumerable<UploadablePlayer> uploadedPlayers) {
        var contentIds = uploadedPlayers
            .Select(static player => player.ContentId)
            .Distinct()
            .ToArray();

        if (contentIds.Length == 0) {
            return;
        }

        lock (_sync) {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE players
                SET dirty = 0,
                    last_uploaded_utc = @uploaded_at
                WHERE content_id = @content_id;
                """;

            var uploadedAtParam = command.Parameters.Add("@uploaded_at", SqliteType.Text);
            var contentIdParam = command.Parameters.Add("@content_id", SqliteType.Integer);
            uploadedAtParam.Value = DateTimeOffset.UtcNow.ToString("O");

            foreach (var contentId in contentIds) {
                contentIdParam.Value = (long)contentId;
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
    }

    public int MarkAllPlayersDirty() {
        lock (_sync) {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = """
                UPDATE players
                SET dirty = 1;
                """;

            var affected = command.ExecuteNonQuery();
            return Math.Max(affected, 0);
        }
    }

    public bool TryGetIdentity(ulong contentId, [NotNullWhen(true)] out CharacterIdentitySnapshot? snapshot) {
        lock (_sync) {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT name, home_world, world_name, last_resolved_utc
                FROM player_identities
                WHERE content_id = @content_id
                  AND name IS NOT NULL
                  AND home_world IS NOT NULL
                  AND world_name IS NOT NULL
                  AND last_resolved_utc IS NOT NULL
                LIMIT 1;
                """;
            command.Parameters.AddWithValue("@content_id", (long)contentId);

            using var reader = command.ExecuteReader();
            if (!reader.Read()) {
                snapshot = null;
                return false;
            }

            snapshot = new CharacterIdentitySnapshot(
                contentId,
                reader.GetString(0),
                Convert.ToUInt16(reader.GetInt64(1)),
                reader.GetString(2),
                ParseTimestamp(reader.GetString(3))
            );
            return true;
        }
    }

    public void UpsertResolvedIdentity(CharacterIdentitySnapshot snapshot) {
        lock (_sync) {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO player_identities (
                    content_id,
                    name,
                    home_world,
                    world_name,
                    last_resolved_utc,
                    pending_upload,
                    last_uploaded_utc
                )
                VALUES (
                    @content_id,
                    @name,
                    @home_world,
                    @world_name,
                    @last_resolved_utc,
                    1,
                    NULL
                )
                ON CONFLICT(content_id) DO UPDATE SET
                    name = excluded.name,
                    home_world = excluded.home_world,
                    world_name = excluded.world_name,
                    last_resolved_utc = excluded.last_resolved_utc,
                    pending_upload = 1,
                    last_uploaded_utc = NULL;
                """;

            command.Parameters.AddWithValue("@content_id", (long)snapshot.ContentId);
            command.Parameters.AddWithValue("@name", snapshot.Name);
            command.Parameters.AddWithValue("@home_world", (long)snapshot.HomeWorld);
            command.Parameters.AddWithValue("@world_name", snapshot.WorldName);
            command.Parameters.AddWithValue("@last_resolved_utc", snapshot.LastResolvedAtUtc.ToString("O"));
            command.ExecuteNonQuery();
        }
    }

    public List<CharacterIdentitySnapshot> TakePendingIdentityUploads(int maxBatchSize) {
        lock (_sync) {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT content_id, name, home_world, world_name, last_resolved_utc
                FROM player_identities
                WHERE pending_upload = 1
                ORDER BY last_resolved_utc DESC, content_id ASC
                LIMIT @limit;
                """;
            command.Parameters.AddWithValue("@limit", Math.Clamp(maxBatchSize, 1, MaxBatchSize));

            var snapshots = new List<CharacterIdentitySnapshot>();
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                snapshots.Add(new CharacterIdentitySnapshot(
                    (ulong)reader.GetInt64(0),
                    reader.GetString(1),
                    Convert.ToUInt16(reader.GetInt64(2)),
                    reader.GetString(3),
                    ParseTimestamp(reader.GetString(4))
                ));
            }

            return snapshots;
        }
    }

    public void MarkIdentityUploadsSubmitted(IEnumerable<ulong> contentIds) {
        var distinctContentIds = contentIds
            .Distinct()
            .ToArray();

        if (distinctContentIds.Length == 0) {
            return;
        }

        lock (_sync) {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE player_identities
                SET pending_upload = 0,
                    last_uploaded_utc = @uploaded_at
                WHERE content_id = @content_id;
                """;

            var uploadedAtParam = command.Parameters.Add("@uploaded_at", SqliteType.Text);
            var contentIdParam = command.Parameters.Add("@content_id", SqliteType.Integer);
            uploadedAtParam.Value = DateTimeOffset.UtcNow.ToString("O");

            foreach (var contentId in distinctContentIds) {
                contentIdParam.Value = (long)contentId;
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
    }

    public void Dispose() {
        _disposed = true;
    }

    private void Initialize() {
        lock (_sync) {
            using var connection = OpenConnection();
            using var pragmaCommand = connection.CreateCommand();
            pragmaCommand.CommandText = """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                """;
            pragmaCommand.ExecuteNonQuery();

            EnsurePlayersTable(connection);
            MigrateIdentityColumns(connection);
            EnsureSchemaVersion(connection);
        }
    }

    private SqliteConnection OpenConnection() {
        if (_disposed) {
            throw new ObjectDisposedException(nameof(PlayerLocalDatabase));
        }

        var connection = new SqliteConnection($"Data Source={_databasePath};Mode=ReadWriteCreate;Cache=Shared");
        connection.Open();
        return connection;
    }

    private static void EnsurePlayersTable(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS players (
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
            CREATE INDEX IF NOT EXISTS idx_players_dirty_last_seen ON players(dirty, last_seen_utc);
            """;
        command.ExecuteNonQuery();
    }

    private static void MigrateIdentityColumns(SqliteConnection connection) {
        if (!TableExists(connection, IdentitiesTableName)) {
            using var createTableCommand = connection.CreateCommand();
            createTableCommand.CommandText = """
                CREATE TABLE player_identities (
                    content_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    home_world INTEGER NOT NULL,
                    world_name TEXT NOT NULL,
                    last_resolved_utc TEXT NOT NULL,
                    pending_upload INTEGER NOT NULL DEFAULT 0,
                    last_uploaded_utc TEXT
                );
                """;
            createTableCommand.ExecuteNonQuery();
        } else {
            var existingColumns = GetColumnNames(connection, IdentitiesTableName);

            AddColumnIfMissing(connection, existingColumns, IdentitiesTableName, "name", "TEXT");
            AddColumnIfMissing(connection, existingColumns, IdentitiesTableName, "home_world", "INTEGER");
            AddColumnIfMissing(connection, existingColumns, IdentitiesTableName, "world_name", "TEXT");
            AddColumnIfMissing(connection, existingColumns, IdentitiesTableName, "last_resolved_utc", "TEXT");
            AddColumnIfMissing(connection, existingColumns, IdentitiesTableName, "pending_upload", "INTEGER NOT NULL DEFAULT 0");
            AddColumnIfMissing(connection, existingColumns, IdentitiesTableName, "last_uploaded_utc", "TEXT");
        }

        using var createIndexCommand = connection.CreateCommand();
        createIndexCommand.CommandText = """
            CREATE INDEX IF NOT EXISTS idx_player_identities_pending_upload_last_resolved
            ON player_identities(pending_upload, last_resolved_utc);
            """;
        createIndexCommand.ExecuteNonQuery();
    }

    private static void EnsureSchemaVersion(SqliteConnection connection) {
        using var getVersionCommand = connection.CreateCommand();
        getVersionCommand.CommandText = "PRAGMA user_version;";
        var currentVersion = Convert.ToInt32(getVersionCommand.ExecuteScalar() ?? 0);

        if (currentVersion >= CurrentSchemaVersion) {
            return;
        }

        using var setVersionCommand = connection.CreateCommand();
        setVersionCommand.CommandText = $"PRAGMA user_version = {CurrentSchemaVersion};";
        setVersionCommand.ExecuteNonQuery();
    }

    private static bool TableExists(SqliteConnection connection, string tableName) {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table' AND name = @table_name;
            """;
        command.Parameters.AddWithValue("@table_name", tableName);
        var result = command.ExecuteScalar();
        return result is long count && count > 0;
    }

    private static HashSet<string> GetColumnNames(SqliteConnection connection, string tableName) {
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({tableName});";

        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            columns.Add(reader.GetString(1));
        }

        return columns;
    }

    private static void AddColumnIfMissing(
        SqliteConnection connection,
        ISet<string> existingColumns,
        string tableName,
        string columnName,
        string columnDefinition
    ) {
        if (existingColumns.Contains(columnName)) {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnDefinition};";
        command.ExecuteNonQuery();
        existingColumns.Add(columnName);
    }

    private static string ComputePayloadHash(UploadablePlayer player) {
        var canonical = string.Concat(
            player.ContentId,
            "|",
            player.Name,
            "|",
            player.HomeWorld,
            "|",
            player.CurrentWorld,
            "|",
            player.AccountId
        );

        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToBase64String(hash);
    }

    private static DateTime ParseTimestamp(string value) {
        return DateTime.Parse(
            value,
            provider: null,
            System.Globalization.DateTimeStyles.RoundtripKind
        );
    }

    private static void PruneCleanRows(SqliteConnection connection, SqliteTransaction transaction) {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM players
            WHERE dirty = 0
              AND last_seen_utc < @cutoff;
            """;
        command.Parameters.AddWithValue("@cutoff", DateTimeOffset.UtcNow.AddDays(-14).ToString("O"));
        command.ExecuteNonQuery();
    }
}

internal sealed class UploadablePlayer {
    public ulong ContentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }
    public ushort CurrentWorld { get; set; }
    public ulong AccountId { get; set; }
}
