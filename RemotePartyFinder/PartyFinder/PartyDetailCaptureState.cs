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

    internal long? CurrentRequestSerial {
        get {
            lock (_gate) {
                return _nextRequestSerial > 0 ? _nextRequestSerial : null;
            }
        }
    }

    internal bool HasActiveRequest {
        get {
            lock (_gate) {
                return TryGetCurrentCycle(out _);
            }
        }
    }

    internal PartyDetailRequestOwner? CurrentOwner {
        get {
            lock (_gate) {
                return TryGetCurrentCycle(out var cycle) ? cycle.Owner : null;
            }
        }
    }

    internal uint CurrentListingId {
        get {
            lock (_gate) {
                return TryGetCurrentCycle(out var cycle) ? cycle.ListingId : 0U;
            }
        }
    }

    internal ulong CurrentContentId {
        get {
            lock (_gate) {
                return TryGetCurrentCycle(out var cycle) ? cycle.ContentId : 0UL;
            }
        }
    }

    internal PartyDetailRequestCycle BeginRequest(PartyDetailRequestOwner owner, uint listingId, ulong contentId) {
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
            _latestArrival = new PartyDetailArrival(generation, cycle, CloneSnapshot(snapshot));
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

            var requestContentMatches = _latestArrival.Cycle.ContentId == 0
                                        || _latestArrival.Cycle.ContentId == contentId;
            var snapshotContentMatches = contentId == 0
                                         || _latestArrival.Snapshot.LeaderContentId == contentId;

            return _latestArrival.Generation > _latestConsumedGeneration
                   && _latestArrival.Cycle.RequestSerial == requestSerial
                   && _latestArrival.Cycle.Owner == PartyDetailRequestOwner.Scanner
                   && _latestArrival.Cycle.ListingId == listingId
                   && _latestArrival.Snapshot.ListingId == listingId
                   && requestContentMatches
                   && snapshotContentMatches;
        }
    }

    public bool IsScannerConsumedAckReady(long requestSerial, uint listingId, ulong contentId) {
        lock (_gate) {
            if (!_hasLatestArrival || _latestArrival.Generation != _latestConsumedGeneration) {
                return false;
            }

            var requestContentMatches = _latestArrival.Cycle.ContentId == 0
                                        || _latestArrival.Cycle.ContentId == contentId;
            var snapshotContentMatches = contentId == 0
                                         || _latestArrival.Snapshot.LeaderContentId == contentId;

            return _latestArrival.Cycle.RequestSerial == requestSerial
                   && _latestArrival.Cycle.Owner == PartyDetailRequestOwner.Scanner
                   && _latestArrival.Cycle.ListingId == listingId
                   && _latestArrival.Snapshot.ListingId == listingId
                   && requestContentMatches
                   && snapshotContentMatches;
        }
    }

    internal bool TryGetCurrentRequestCycle(out PartyDetailRequestCycle cycle) {
        lock (_gate) {
            return TryGetCurrentCycle(out cycle);
        }
    }

    internal bool TryGetNextUnconsumedArrival(out PartyDetailPendingArrival arrival) {
        lock (_gate) {
            if (!_hasLatestArrival || _latestArrival.Generation <= _latestConsumedGeneration) {
                arrival = default;
                return false;
            }

            arrival = new PartyDetailPendingArrival(
                _latestArrival.Generation,
                _latestArrival.Cycle,
                CloneSnapshot(_latestArrival.Snapshot)
            );
            return true;
        }
    }

    private static UploadablePartyDetail CloneSnapshot(UploadablePartyDetail snapshot) {
        return new UploadablePartyDetail {
            ListingId = snapshot.ListingId,
            LeaderContentId = snapshot.LeaderContentId,
            LeaderName = snapshot.LeaderName,
            HomeWorld = snapshot.HomeWorld,
            MemberContentIds = snapshot.MemberContentIds is { } memberContentIds ? new List<ulong>(memberContentIds) : new List<ulong>(),
            MemberJobs = snapshot.MemberJobs is { } memberJobs ? new List<byte>(memberJobs) : new List<byte>(),
            SlotFlags = snapshot.SlotFlags is { } slotFlags ? new List<string>(slotFlags) : new List<string>(),
        };
    }

    private bool TryGetCurrentCycle(out PartyDetailRequestCycle cycle) {
        if (_nextRequestSerial <= 0) {
            cycle = default;
            return false;
        }

        return _pendingRequests.TryGetValue(_nextRequestSerial, out cycle);
    }

    private sealed record PartyDetailArrival(long Generation, PartyDetailRequestCycle Cycle, UploadablePartyDetail Snapshot);
}
