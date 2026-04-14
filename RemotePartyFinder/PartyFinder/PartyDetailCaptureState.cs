using System;
using System.Collections.Generic;

namespace RemotePartyFinder;

internal sealed class PartyDetailCaptureState {
    private readonly object _gate = new();
    private readonly Dictionary<long, PartyDetailRequestCycle> _pendingRequests = new();

    private long _nextRequestSerial;
    private long _latestArrivalGeneration;
    private long _latestConsumedGeneration;
    private PartyDetailArrival _latestArrival = new(0, default, new UploadablePartyDetail());
    private bool _hasLatestArrival;

    internal long LatestArrivalGeneration {
        get {
            lock (_gate) {
                return _latestArrivalGeneration;
            }
        }
    }

    internal long LastConsumedGeneration {
        get {
            lock (_gate) {
                return _latestConsumedGeneration;
            }
        }
    }

    public PartyDetailRequestCycle BeginRequest(PartyDetailRequestOwner owner, uint listingId, ulong contentId) {
        lock (_gate) {
            var cycle = new PartyDetailRequestCycle(++_nextRequestSerial, owner, listingId, contentId);
            _pendingRequests[cycle.RequestSerial] = cycle;
            return cycle;
        }
    }

    public bool TryRecordArrival(long requestSerial, UploadablePartyDetail snapshot) {
        ArgumentNullException.ThrowIfNull(snapshot);

        lock (_gate) {
            if (!_pendingRequests.Remove(requestSerial, out var cycle)) {
                return false;
            }

            var generation = ++_latestArrivalGeneration;
            _latestArrival = new PartyDetailArrival(generation, cycle, snapshot);
            _hasLatestArrival = true;
            return true;
        }
    }

    public bool TryMarkConsumed(long generation) {
        if (generation <= 0) {
            return false;
        }

        lock (_gate) {
            if (generation > _latestArrivalGeneration || generation <= _latestConsumedGeneration) {
                return false;
            }

            _latestConsumedGeneration = generation;
            return true;
        }
    }

    public bool IsScannerAckReady(long requestSerial, uint listingId, ulong contentId) {
        lock (_gate) {
            if (!_hasLatestArrival) {
                return false;
            }

            return _latestArrival.Generation > _latestConsumedGeneration
                   && _latestArrival.Cycle.RequestSerial == requestSerial
                   && _latestArrival.Cycle.Owner == PartyDetailRequestOwner.Scanner
                   && _latestArrival.Cycle.ListingId == listingId
                   && _latestArrival.Cycle.ContentId == contentId
                   && _latestArrival.Snapshot.ListingId == listingId
                   && _latestArrival.Snapshot.LeaderContentId == contentId;
        }
    }

    private sealed record PartyDetailArrival(long Generation, PartyDetailRequestCycle Cycle, UploadablePartyDetail Snapshot);
}
