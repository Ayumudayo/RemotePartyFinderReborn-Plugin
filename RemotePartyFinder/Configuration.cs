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
    
    // UI Collapse States (not saved, just runtime state)
    [NonSerialized]
    public bool CollapseFFLogsSettings = false;
    [NonSerialized]
    public bool CollapseAdvancedSettings = false;
    [NonSerialized]
    public bool CollapseDebugSettings = false;

    public static ImmutableList<UploadUrl> DefaultUploadUrls() => [
        new("http://127.0.0.1:8000") { IsDefault = true }
    ];

    public void Save() {
        Plugin.PluginInterface.SavePluginConfig(this);
    }
}
