using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

internal enum ProtectedEndpointCapabilityKind
{
    FflogsJobs,
    FflogsResults,
    FflogsLeasesAbandon,
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class ProtectedEndpointCapabilityGrant
{
    public string Token { get; set; } = string.Empty;
    public long ExpiresAt { get; set; }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class ProtectedEndpointCapabilities
{
    public ProtectedEndpointCapabilityGrant FflogsJobs { get; set; } = new();
    public ProtectedEndpointCapabilityGrant FflogsResults { get; set; } = new();
    public ProtectedEndpointCapabilityGrant FflogsLeasesAbandon { get; set; } = new();
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class ListingDetailCapability
{
    public uint ListingId { get; set; }
    public string Token { get; set; } = string.Empty;
    public long ExpiresAt { get; set; }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class ContributeMultipleResponse
{
    public string Status { get; set; } = string.Empty;
    public int Requested { get; set; }
    public int Accepted { get; set; }
    public int Updated { get; set; }
    public int Failed { get; set; }
    public List<ListingDetailCapability> DetailCapabilities { get; set; } = [];
    public ProtectedEndpointCapabilities ProtectedEndpoints { get; set; }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class ContributePlayersResponse
{
    public string Status { get; set; } = string.Empty;
    public int Requested { get; set; }
    public int Accepted { get; set; }
    public int Updated { get; set; }
    public int Invalid { get; set; }
    public int Failed { get; set; }
    public ProtectedEndpointCapabilities ProtectedEndpoints { get; set; }
}

internal static class IngestResponseParser
{
    public static void CaptureMultipleResponse(UploadUrl uploadUrl, string responseBody)
    {
        if (!TryDeserialize(responseBody, out ContributeMultipleResponse parsed))
        {
            return;
        }

        uploadUrl.ApplyIngestCapabilities(parsed.ProtectedEndpoints, parsed.DetailCapabilities);
    }

    public static void CapturePlayersResponse(UploadUrl uploadUrl, string responseBody)
    {
        if (!TryDeserialize(responseBody, out ContributePlayersResponse parsed))
        {
            return;
        }

        uploadUrl.ApplyIngestCapabilities(parsed.ProtectedEndpoints, null);
    }

    private static bool TryDeserialize<T>(string responseBody, out T parsed)
        where T : class
    {
        parsed = null;

        if (string.IsNullOrWhiteSpace(responseBody))
        {
            return false;
        }

        var trimmed = responseBody.TrimStart();
        if (!trimmed.StartsWith('{'))
        {
            return false;
        }

        try
        {
            parsed = JsonConvert.DeserializeObject<T>(responseBody);
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }
}

internal static class IngestEndpointResolver
{
    private static readonly TimeSpan WarningCooldown = TimeSpan.FromMinutes(5);

    public static bool TryBuildEndpointUrl(UploadUrl uploadUrl, string endpointPath, out string endpointUrl)
    {
        endpointUrl = string.Empty;
        if (!TryResolveBaseUrl(uploadUrl.Url, out var baseUrl, out var error))
        {
            LogBlockedUrl(uploadUrl, error);
            return false;
        }

        endpointUrl = baseUrl + endpointPath;
        return true;
    }

    public static bool IsValidUploadUrl(string configuredUrl, out string error)
        => TryResolveBaseUrl(configuredUrl, out _, out error);

    private static bool TryResolveBaseUrl(string configuredUrl, out string baseUrl, out string error)
    {
        baseUrl = string.Empty;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(configuredUrl))
        {
            error = "Upload URL is empty.";
            return false;
        }

        if (!Uri.TryCreate(configuredUrl.Trim(), UriKind.Absolute, out var uri))
        {
            error = "Invalid URL format.";
            return false;
        }

        if (uri.Scheme != Uri.UriSchemeHttps && uri.Scheme != Uri.UriSchemeHttp)
        {
            error = "Only http:// or https:// URLs are supported.";
            return false;
        }

        if (uri.Scheme == Uri.UriSchemeHttp && !IsLoopbackHost(uri))
        {
            error = "Remote HTTP upload URLs are blocked. Use HTTPS or localhost HTTP only.";
            return false;
        }

        baseUrl = uri.GetLeftPart(UriPartial.Path).TrimEnd('/');
        if (baseUrl.EndsWith("/contribute/multiple", StringComparison.OrdinalIgnoreCase))
        {
            baseUrl = baseUrl[..^"/contribute/multiple".Length];
        }
        else if (baseUrl.EndsWith("/contribute", StringComparison.OrdinalIgnoreCase))
        {
            baseUrl = baseUrl[..^"/contribute".Length];
        }

        return true;
    }

    private static bool IsLoopbackHost(Uri uri)
    {
        if (uri.IsLoopback)
        {
            return true;
        }

        if (IPAddress.TryParse(uri.Host, out var ipAddress))
        {
            return IPAddress.IsLoopback(ipAddress);
        }

        return string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase);
    }

    private static void LogBlockedUrl(UploadUrl uploadUrl, string error)
    {
        var now = DateTime.UtcNow;
        if (!uploadUrl.ShouldLogSecurityWarning(now, WarningCooldown))
        {
            return;
        }

        Plugin.Log.Warning($"RemotePartyFinder: skipped upload target '{uploadUrl.Url}': {error}");
    }
}
