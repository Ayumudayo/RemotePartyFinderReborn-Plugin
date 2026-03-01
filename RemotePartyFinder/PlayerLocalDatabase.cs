using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace RemotePartyFinder;

internal sealed class PlayerLocalDatabase : IDisposable {
    public const int MaxBatchSize = 100;

    private readonly string _databasePath;
    private readonly object _sync = new();
    private bool _disposed;

    public PlayerLocalDatabase(Plugin plugin) {
        _databasePath = Path.Combine(Plugin.PluginInterface.ConfigDirectory.FullName, "player_cache.db");
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
            command.CommandText = @"
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
    END;";

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
            command.CommandText = @"
SELECT content_id, name, home_world, current_world, account_id
FROM players
WHERE dirty = 1
ORDER BY last_seen_utc DESC
LIMIT @limit;";
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
            .Select(p => p.ContentId)
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
            command.CommandText = @"
UPDATE players
SET dirty = 0,
    last_uploaded_utc = @uploaded_at
WHERE content_id = @content_id;";

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
            command.CommandText = @"
UPDATE players
SET dirty = 1;";

            var affected = command.ExecuteNonQuery();
            return Math.Max(affected, 0);
        }
    }

    public void Dispose() {
        _disposed = true;
    }

    private void Initialize() {
        lock (_sync) {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = @"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
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
CREATE INDEX IF NOT EXISTS idx_players_dirty_last_seen ON players(dirty, last_seen_utc);";
            command.ExecuteNonQuery();
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

    private static void PruneCleanRows(SqliteConnection connection, SqliteTransaction transaction) {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
DELETE FROM players
WHERE dirty = 0
  AND last_seen_utc < @cutoff;";
        command.Parameters.AddWithValue("@cutoff", DateTimeOffset.UtcNow.AddDays(-14).ToString("O"));
        command.ExecuteNonQuery();
    }
}
