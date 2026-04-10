using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace RemotePartyFinder;

public record UploadUrl(string Url)
{
    private sealed record CachedCapability(string Token, DateTimeOffset ExpiresAtUtc);

    public string Url { get; set; } = Url;
    public bool IsDefault { get; init; }
    public bool IsEnabled { get; set; } = true;

    [JsonIgnore]
    public int FailureCount { get; set; }

    [JsonIgnore]
    public DateTime LastFailureTime { get; set; }

    [JsonIgnore]
    public DateTime LastSecurityWarningTime { get; set; }

    [JsonIgnore]
    private readonly object _capabilityLock = new();

    [JsonIgnore]
    private readonly Dictionary<ProtectedEndpointCapabilityKind, CachedCapability> _protectedEndpointCapabilities = new();

    [JsonIgnore]
    private readonly Dictionary<uint, CachedCapability> _detailCapabilities = new();

    [JsonIgnore]
    private bool _requiresProtectedEndpointCapabilities;

    [JsonIgnore]
    private bool _requiresDetailCapabilities;

    internal bool ShouldLogSecurityWarning(DateTime utcNow, TimeSpan cooldown)
    {
        if (utcNow - LastSecurityWarningTime < cooldown)
        {
            return false;
        }

        LastSecurityWarningTime = utcNow;
        return true;
    }

    internal void ApplyIngestCapabilities(
        ProtectedEndpointCapabilities protectedEndpoints,
        IEnumerable<ListingDetailCapability> detailCapabilities)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_capabilityLock)
        {
            PruneExpiredCapabilities(now);

            _requiresProtectedEndpointCapabilities |= UpsertProtectedCapability(
                ProtectedEndpointCapabilityKind.FflogsJobs,
                protectedEndpoints?.FflogsJobs,
                now);
            _requiresProtectedEndpointCapabilities |= UpsertProtectedCapability(
                ProtectedEndpointCapabilityKind.FflogsResults,
                protectedEndpoints?.FflogsResults,
                now);
            _requiresProtectedEndpointCapabilities |= UpsertProtectedCapability(
                ProtectedEndpointCapabilityKind.FflogsLeasesAbandon,
                protectedEndpoints?.FflogsLeasesAbandon,
                now);

            if (detailCapabilities == null)
            {
                return;
            }

            foreach (var detailCapability in detailCapabilities)
            {
                if (detailCapability == null || detailCapability.ListingId == 0)
                {
                    continue;
                }

                var cached = BuildCachedCapability(detailCapability.Token, detailCapability.ExpiresAt, now);
                if (cached == null)
                {
                    continue;
                }

                _detailCapabilities[detailCapability.ListingId] = cached;
                _requiresDetailCapabilities = true;
            }
        }
    }

    internal bool ShouldDeferProtectedEndpointRequest(ProtectedEndpointCapabilityKind kind)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_capabilityLock)
        {
            PruneExpiredCapabilities(now);
            return _requiresProtectedEndpointCapabilities
                && !_protectedEndpointCapabilities.ContainsKey(kind);
        }
    }

    internal bool ShouldDeferDetailRequest(uint listingId)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_capabilityLock)
        {
            PruneExpiredCapabilities(now);
            return _requiresDetailCapabilities
                && !_detailCapabilities.ContainsKey(listingId);
        }
    }

    internal void MarkProtectedEndpointCapabilitiesRequired()
    {
        lock (_capabilityLock)
        {
            _requiresProtectedEndpointCapabilities = true;
        }
    }

    internal void MarkDetailCapabilitiesRequired()
    {
        lock (_capabilityLock)
        {
            _requiresDetailCapabilities = true;
        }
    }

    internal bool TryGetProtectedEndpointCapability(ProtectedEndpointCapabilityKind kind, out string token)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_capabilityLock)
        {
            PruneExpiredCapabilities(now);
            if (_protectedEndpointCapabilities.TryGetValue(kind, out var cached))
            {
                token = cached.Token;
                return true;
            }
        }

        token = string.Empty;
        return false;
    }

    internal void InvalidateProtectedEndpointCapability(ProtectedEndpointCapabilityKind kind)
    {
        lock (_capabilityLock)
        {
            _protectedEndpointCapabilities.Remove(kind);
        }
    }

    internal bool TryGetDetailCapability(uint listingId, out string token)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_capabilityLock)
        {
            PruneExpiredCapabilities(now);
            if (_detailCapabilities.TryGetValue(listingId, out var cached))
            {
                token = cached.Token;
                return true;
            }
        }

        token = string.Empty;
        return false;
    }

    internal void InvalidateDetailCapability(uint listingId)
    {
        lock (_capabilityLock)
        {
            _detailCapabilities.Remove(listingId);
        }
    }

    private bool UpsertProtectedCapability(
        ProtectedEndpointCapabilityKind kind,
        ProtectedEndpointCapabilityGrant grant,
        DateTimeOffset now)
    {
        var cached = BuildCachedCapability(grant?.Token, grant?.ExpiresAt ?? 0, now);
        if (cached == null)
        {
            return false;
        }

        _protectedEndpointCapabilities[kind] = cached;
        return true;
    }

    private static CachedCapability BuildCachedCapability(
        string token,
        long expiresAtUnixSeconds,
        DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(token) || expiresAtUnixSeconds <= 0)
        {
            return null;
        }

        DateTimeOffset expiresAtUtc;
        try
        {
            expiresAtUtc = DateTimeOffset.FromUnixTimeSeconds(expiresAtUnixSeconds);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }

        if (expiresAtUtc <= now)
        {
            return null;
        }

        return new CachedCapability(token, expiresAtUtc);
    }

    private void PruneExpiredCapabilities(DateTimeOffset now)
    {
        foreach (var (kind, cached) in _protectedEndpointCapabilities.ToArray())
        {
            if (cached.ExpiresAtUtc <= now)
            {
                _protectedEndpointCapabilities.Remove(kind);
            }
        }

        foreach (var (listingId, cached) in _detailCapabilities.ToArray())
        {
            if (cached.ExpiresAtUtc <= now)
            {
                _detailCapabilities.Remove(listingId);
            }
        }
    }
}
