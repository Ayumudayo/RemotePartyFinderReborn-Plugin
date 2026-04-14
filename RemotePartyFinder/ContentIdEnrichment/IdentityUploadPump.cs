using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace RemotePartyFinder;

internal sealed class IdentityUploadPump {
    private static readonly TimeSpan[] DefaultRetryBackoffSchedule = [
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(1),
    ];

    private readonly PlayerLocalDatabase _database;
    private readonly Func<DateTime> _utcNow;
    private readonly Func<IReadOnlyList<CharacterIdentityUploadPayload>, CancellationToken, Task<bool>> _uploadResolvedIdentitiesAsync;
    private readonly CancellationToken _disposeToken;
    private readonly Action<string>? _debugSink;
    private readonly Action<string>? _warningSink;
    private readonly TimeSpan[] _retryBackoffSchedule;
    private readonly object _sync = new();
    private int _identityUploadWorkerBusy;
    private int _identityUploadFailureCount;
    private DateTime _nextIdentityUploadAttemptAtUtc = DateTime.MinValue;

    internal IdentityUploadPump(
        PlayerLocalDatabase database,
        Func<DateTime> utcNow,
        Func<IReadOnlyList<CharacterIdentityUploadPayload>, CancellationToken, Task<bool>> uploadResolvedIdentitiesAsync,
        CancellationToken disposeToken,
        Action<string>? debugSink = null,
        Action<string>? warningSink = null,
        IReadOnlyList<TimeSpan>? retryBackoffSchedule = null
    ) {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _utcNow = utcNow ?? throw new ArgumentNullException(nameof(utcNow));
        _uploadResolvedIdentitiesAsync = uploadResolvedIdentitiesAsync ?? throw new ArgumentNullException(nameof(uploadResolvedIdentitiesAsync));
        _disposeToken = disposeToken;
        _debugSink = debugSink;
        _warningSink = warningSink;
        _retryBackoffSchedule = retryBackoffSchedule?.ToArray() ?? DefaultRetryBackoffSchedule;
    }

    internal bool IsWorkerBusy => Volatile.Read(ref _identityUploadWorkerBusy) != 0;

    internal int FailureCount {
        get {
            lock (_sync) {
                return _identityUploadFailureCount;
            }
        }
    }

    internal DateTime NextAttemptAtUtc {
        get {
            lock (_sync) {
                return _nextIdentityUploadAttemptAtUtc;
            }
        }
    }

    internal void PumpPendingIdentityUploads() {
        lock (_sync) {
            if (_utcNow() < _nextIdentityUploadAttemptAtUtc) {
                return;
            }
        }

        if (Interlocked.CompareExchange(ref _identityUploadWorkerBusy, 1, 0) != 0) {
            return;
        }

        if (_database.TakePendingIdentityUploads(1).Count == 0) {
            Interlocked.Exchange(ref _identityUploadWorkerBusy, 0);
            return;
        }

        _ = Task.Run(async () => {
            var uploadSucceeded = false;
            try {
                uploadSucceeded = await DrainPendingIdentityUploadsOnceAsync(_disposeToken).ConfigureAwait(false);
            } catch (OperationCanceledException) when (_disposeToken.IsCancellationRequested) {
            } catch (Exception exception) {
                _warningSink?.Invoke($"CharaCardResolver: identity upload worker failed. {exception.Message}");
            } finally {
                if (!_disposeToken.IsCancellationRequested) {
                    RecordIdentityUploadAttempt(uploadSucceeded, _utcNow());
                }

                Interlocked.Exchange(ref _identityUploadWorkerBusy, 0);
            }
        }, _disposeToken);
    }

    internal async Task<bool> DrainPendingIdentityUploadsOnceAsync(CancellationToken cancellationToken) {
        cancellationToken.ThrowIfCancellationRequested();

        var snapshots = _database.TakePendingIdentityUploads(PlayerLocalDatabase.MaxBatchSize);
        if (snapshots.Count == 0) {
            return false;
        }

        var payloads = snapshots
            .Select(static snapshot => CharacterIdentityUploadPayload.FromSnapshot(snapshot, snapshot.LastResolvedAtUtc))
            .ToArray();

        bool uploadSucceeded;
        try {
            uploadSucceeded = await _uploadResolvedIdentitiesAsync(payloads, cancellationToken).ConfigureAwait(false);
        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || _disposeToken.IsCancellationRequested) {
            return false;
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to upload resolved identities. {exception.Message}");
            return false;
        }

        if (!uploadSucceeded) {
            return false;
        }

        try {
            _database.MarkIdentityUploadsSubmitted(payloads.Select(static payload => payload.ContentId));
            _debugSink?.Invoke($"CharaCardResolver: uploaded resolved identity batch count={payloads.Count()}.");
            return true;
        } catch (Exception exception) {
            _warningSink?.Invoke($"CharaCardResolver: failed to mark identity uploads as submitted. {exception.Message}");
            return false;
        }
    }

    private void RecordIdentityUploadAttempt(bool uploadSucceeded, DateTime observedAtUtc) {
        lock (_sync) {
            if (uploadSucceeded) {
                _identityUploadFailureCount = 0;
                _nextIdentityUploadAttemptAtUtc = DateTime.MinValue;
                return;
            }

            var nextDelay = _retryBackoffSchedule[Math.Min(_identityUploadFailureCount, _retryBackoffSchedule.Length - 1)];
            _identityUploadFailureCount++;
            _nextIdentityUploadAttemptAtUtc = observedAtUtc.Add(nextDelay);
        }
    }
}
