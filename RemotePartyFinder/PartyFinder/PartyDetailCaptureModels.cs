namespace RemotePartyFinder;

internal enum PartyDetailRequestOwner {
    Manual,
    Scanner,
}

internal readonly record struct PartyDetailRequestCycle(
    long RequestSerial,
    PartyDetailRequestOwner Owner,
    uint ListingId,
    ulong ContentId
);
