using System;
using System.Collections.Immutable;
using Dalamud.Configuration;

namespace RemotePartyFinder;

[Serializable]
public class Configuration : IPluginConfiguration {
    public int Version { get; set; } = 1;
    public bool AdvancedSettingsEnabled = false;
    
    // Circuit Breaker Settings
    public int CircuitBreakerFailureThreshold { get; set; } = 3;
    public int CircuitBreakerBreakDurationMinutes { get; set; } = 1;
    
    public ImmutableList<UploadUrl> UploadUrls = DefaultUploadUrls();

    // FFLogs API Settings
    public string FFLogsClientId { get; set; } = string.Empty;
    public string FFLogsClientSecret { get; set; } = string.Empty;
    public bool EnableFFLogsWorker { get; set; } = true;
    public int FFLogsWorkerBaseDelayMs { get; set; } = 5000;
    public int FFLogsWorkerIdleDelayMs { get; set; } = 10000;
    public int FFLogsWorkerMaxBackoffDelayMs { get; set; } = 60000;
    public int FFLogsWorkerJitterMs { get; set; } = 2000;

    // Ingest security metadata
    public string IngestClientId { get; set; } = string.Empty;
    public string IngestSharedSecret { get; set; } = "rpf-reborn-public-ingest-v1";

    // Debug scanner: auto-open PF detail windows
    public bool EnableAutoDetailScanDebug { get; set; } = false;
    public bool AutoDetailScanCurrentPageOnly { get; set; } = true;
    public int AutoDetailScanNextPageButtonId { get; set; } = 0;
    public int AutoDetailScanNextPageActionId { get; set; } = 22;
    public int AutoDetailScanActionIntervalMs { get; set; } = 400;
    public int AutoDetailScanDetailTimeoutMs { get; set; } = 3500;
    public int AutoDetailScanMinDwellMs { get; set; } = 800;
    public int AutoDetailScanPostListingCooldownMs { get; set; } = 300;
    public int AutoDetailScanRefreshIntervalMs { get; set; } = 5000;
    public int AutoDetailScanDedupTtlSeconds { get; set; } = 600;
    public int AutoDetailScanMaxConsecutiveFailures { get; set; } = 5;
    public int AutoDetailScanMaxPerRun { get; set; } = 0;
    
    public static ImmutableList<UploadUrl> DefaultUploadUrls() => [
        new("http://127.0.0.1:8000") { IsDefault = true }
    ];

    public void Save() {
        Plugin.PluginInterface.SavePluginConfig(this);
    }
}
