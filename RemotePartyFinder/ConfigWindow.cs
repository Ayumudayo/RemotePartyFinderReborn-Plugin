using System;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Dalamud.Interface.Components;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Interface.Windowing;
using Dalamud.Bindings.ImGui;

namespace RemotePartyFinder;

public class ConfigWindow : Window, IDisposable
{
    private readonly Plugin _plugin;
    private readonly Configuration _configuration;
    private string _uploadUrlTempString = string.Empty;
    private string _uploadUrlError = string.Empty;
    private string _playerManualUploadStatus = string.Empty;

    public ConfigWindow(Plugin plugin) : base("Remote Party Finder Reborn")
    {
        _plugin = plugin;
        _configuration = plugin.Configuration;
        Flags = ImGuiWindowFlags.AlwaysAutoResize;

        Size = new Vector2(500, 0);
    }

    public void Dispose()
    {
    }

    public override void OnClose()
    {
        _configuration.Save();
    }

    public override void Draw()
    {
        if (ImGui.BeginTabBar("ConfigTabs"))
        {
            if (ImGui.BeginTabItem("Settings"))
            {
                DrawSettingsTab();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem("Debug"))
            {
                DrawDebugTab();
                ImGui.EndTabItem();
            }

            ImGui.EndTabBar();
        }
    }

    private void DrawSettingsTab()
    {
        // FFLogs API Settings (접기 가능)
        if (ImGui.CollapsingHeader("FFLogs API Settings", ImGuiTreeNodeFlags.DefaultOpen))
        {
            ImGui.Indent();
            
            var clientId = _configuration.FFLogsClientId;
            if (ImGui.InputText("Client ID", ref clientId, 100))
            {
                _configuration.FFLogsClientId = clientId;
                _configuration.Save();
            }

            var clientSecret = _configuration.FFLogsClientSecret;
            if (ImGui.InputText("Client Secret", ref clientSecret, 100, ImGuiInputTextFlags.Password))
            {
                _configuration.FFLogsClientSecret = clientSecret;
                _configuration.Save();
            }

            var enableWorker = _configuration.EnableFFLogsWorker;
            if (ImGui.Checkbox("Enable FFLogs background worker", ref enableWorker))
            {
                _configuration.EnableFFLogsWorker = enableWorker;
                _configuration.Save();
            }

            if (ImGui.CollapsingHeader("How to get a client ID and a client secret:"))
            {
            ImGui.AlignTextToFramePadding();
            ImGui.Bullet();
            ImGui.Text("Open https://www.fflogs.com/api/clients/ or");
            ImGui.SameLine();
            if (ImGui.Button("Click here##APIClientLink"))
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "https://www.fflogs.com/api/clients/",
                    UseShellExecute = true
                });
            }

            ImGui.AlignTextToFramePadding();
            ImGui.Bullet();
            ImGui.Text("Create a new client");

            ImGui.AlignTextToFramePadding();
            ImGui.Bullet();
            ImGui.Text("Choose any name, for example: \"Plugin\"");
            ImGui.SameLine();
            if (ImGui.Button("Copy##APIClientCopyName"))
            {
                CopyToClipboard("Plugin");
            }

            ImGui.AlignTextToFramePadding();
            ImGui.Bullet();
            ImGui.Text("Enter any URL, for example: \"https://www.example.com\"");
            ImGui.SameLine();
            if (ImGui.Button("Copy##APIClientCopyURL"))
            {
                CopyToClipboard("https://www.example.com");
            }

            ImGui.AlignTextToFramePadding();
            ImGui.Bullet();
            ImGui.Text("Do NOT check the Public Client option");

            ImGui.AlignTextToFramePadding();
            ImGui.Bullet();
            ImGui.Text("Paste both client ID and secret above");
            }
            
            ImGui.Unindent();
        }

        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        // Advanced Settings (접기 가능)
        if (ImGui.CollapsingHeader("Advanced Settings"))
        {
            ImGui.Indent();
            
            ImGui.TextWrapped(
                "This section is for advanced users to configure which services to send party finder data to. " +
                "Only enable if you know what you are doing.");
            
            ImGui.Spacing();
            
            var isAdvanced = _configuration.AdvancedSettingsEnabled;
            if (ImGui.Checkbox("Enable Advanced Settings", ref isAdvanced))
            {
                _configuration.AdvancedSettingsEnabled = isAdvanced;
                _configuration.Save();
            }

            if (!isAdvanced) {
                ImGui.Unindent();
                return;
            }

            ImGui.Separator();
            ImGui.Spacing();
        
        using (ImRaii.Table((ImU8String)"uploadUrls", 3, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.Borders))
        {
            ImGui.TableSetupColumn("#", ImGuiTableColumnFlags.WidthFixed);
            ImGui.TableSetupColumn("URL", ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn("Enabled", ImGuiTableColumnFlags.WidthFixed);
            ImGui.TableHeadersRow();
            
            using var id = ImRaii.PushId((ImU8String)"urls");
            foreach (var (uploadUrl, index) in _configuration.UploadUrls.Select((url, index) => (url, index + 1)))
            {
                id.Push(index);

                ImGui.TableNextRow();
                ImGui.TableSetColumnIndex(0);
                ImGui.TextUnformatted(index.ToString());
                
                ImGui.TableSetColumnIndex(1);
                ImGui.TextUnformatted(uploadUrl.Url);

                ImGui.TableSetColumnIndex(2);
                var isEnabled = uploadUrl.IsEnabled;
                if (ImGui.Checkbox("##uploadUrlCheckbox", ref isEnabled))
                {
                    uploadUrl.IsEnabled = isEnabled;
                }

                if (!uploadUrl.IsDefault)
                {
                    ImGui.SameLine();
                    if (ImGuiComponents.IconButton(Dalamud.Interface.FontAwesomeIcon.Trash))
                    {
                        _configuration.UploadUrls = _configuration.UploadUrls.Remove(uploadUrl);
                    }
                }
                
                id.Pop();
            }
            
            ImGui.TableNextRow();
            ImGui.TableSetColumnIndex(1);
            ImGui.SetNextItemWidth(-1);
            ImGui.InputText("##uploadUrlInput", ref _uploadUrlTempString, 300);
            ImGui.TableNextColumn();

            if (!string.IsNullOrEmpty(_uploadUrlTempString) &&
                ImGuiComponents.IconButton(Dalamud.Interface.FontAwesomeIcon.Plus))
            {
                _uploadUrlTempString = _uploadUrlTempString.TrimEnd();

                if (_configuration.UploadUrls.Any(r =>
                        string.Equals(r.Url, _uploadUrlTempString, StringComparison.InvariantCultureIgnoreCase)))
                {
                    _uploadUrlError = "Endpoint already exists.";
                    Task.Delay(5000).ContinueWith(t => _uploadUrlError = string.Empty);
                }
                else if (!ValidUrl(_uploadUrlTempString))
                {
                    this._uploadUrlError = "Invalid URL format.";
                    Task.Delay(5000).ContinueWith(t => _uploadUrlError = string.Empty);
                }
                else
                {
                    _configuration.UploadUrls = _configuration.UploadUrls.Add(new(_uploadUrlTempString));
                    _uploadUrlTempString = string.Empty;
                }
            }
        }

        ImGui.Dummy(new (0, 5));

        if (ImGui.Button("Reset To Default##uploadUrlDefault"))
        {
            ResetToDefault();
        }

            ImGui.SameLine();
            ImGui.TextColored(new Vector4(1, 0, 0, 1), _uploadUrlError);
            
            ImGui.Unindent();
        }
    }

    private void DrawDebugTab()
    {
        ImGui.TextColored(new Vector4(0.4f, 1.0f, 0.4f, 1.0f), "[Circuit Breaker Configuration]");
        ImGui.Spacing();

        var threshold = _configuration.CircuitBreakerFailureThreshold;
        if (ImGui.SliderInt("Failure Threshold", ref threshold, 1, 20, "%d failures"))
        {
            _configuration.CircuitBreakerFailureThreshold = threshold;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Number of consecutive failures before pausing uploads.");
        }

        var duration = _configuration.CircuitBreakerBreakDurationMinutes;
        if (ImGui.SliderInt("Break Duration", ref duration, 1, 60, "%d min"))
        {
            _configuration.CircuitBreakerBreakDurationMinutes = duration;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Duration to pause uploads after threshold is reached.");
        }

        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.TextColored(new Vector4(0.4f, 1.0f, 0.4f, 1.0f), "[FFLogs Worker Timing]");
        ImGui.Spacing();

        var workerBaseDelay = _configuration.FFLogsWorkerBaseDelayMs;
        if (ImGui.SliderInt("Base Delay", ref workerBaseDelay, 1000, 30000, "%d ms"))
        {
            _configuration.FFLogsWorkerBaseDelayMs = workerBaseDelay;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Base delay used for disabled/misconfigured states and exponential backoff base.");
        }

        var workerIdleDelay = _configuration.FFLogsWorkerIdleDelayMs;
        if (ImGui.SliderInt("Idle Delay", ref workerIdleDelay, 1000, 60000, "%d ms"))
        {
            _configuration.FFLogsWorkerIdleDelayMs = workerIdleDelay;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Polling delay when there is no work or after successful processing.");
        }

        var workerMaxBackoffDelay = _configuration.FFLogsWorkerMaxBackoffDelayMs;
        if (ImGui.SliderInt("Max Backoff", ref workerMaxBackoffDelay, 5000, 180000, "%d ms"))
        {
            _configuration.FFLogsWorkerMaxBackoffDelayMs = workerMaxBackoffDelay;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Upper bound for retry backoff after repeated transient failures.");
        }

        var workerJitter = _configuration.FFLogsWorkerJitterMs;
        if (ImGui.SliderInt("Delay Jitter", ref workerJitter, 0, 10000, "%d ms"))
        {
            _configuration.FFLogsWorkerJitterMs = workerJitter;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Random jitter added to worker delays to reduce synchronized polling spikes.");
        }

        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.TextColored(new Vector4(0.4f, 1.0f, 0.4f, 1.0f), "[PF Detail Auto Scanner (Debug)]");
        ImGui.Spacing();

        var enableScanner = _configuration.EnableAutoDetailScanDebug;
        if (ImGui.Checkbox("Enable Auto Detail Scanner", ref enableScanner))
        {
            _configuration.EnableAutoDetailScanDebug = enableScanner;
            if (!enableScanner)
            {
                _plugin.DebugPfScanner.ResetSession();
            }
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Debug mode: automatically opens PF listing details one-by-one to accelerate detail collection.");
        }

        ImGui.TextWrapped("Scanner bootstrap: each run forces a PF listings refresh and starts from page 1 data.");

        var currentPageOnly = _configuration.AutoDetailScanCurrentPageOnly;
        if (ImGui.Checkbox("Current Page Only", ref currentPageOnly))
        {
            _configuration.AutoDetailScanCurrentPageOnly = currentPageOnly;
            _plugin.DebugPfScanner.ResetSession();
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Enabled: only scan listings on the current page. Disabled: automatically turn pages and continue scanning until the last page.");
        }

        var nextPageButtonId = _configuration.AutoDetailScanNextPageButtonId;
        if (ImGui.InputInt("Next Page Button Id (fallback)", ref nextPageButtonId, 1, 5))
        {
            _configuration.AutoDetailScanNextPageButtonId = Math.Clamp(nextPageButtonId, 0, 1000);
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Optional direct-click fallback button id. Default 0 disables direct click and uses callback/replay path first.");
        }

        var nextPageActionId = _configuration.AutoDetailScanNextPageActionId;
        if (ImGui.InputInt("Capture Action Id", ref nextPageActionId, 1, 5))
        {
            _configuration.AutoDetailScanNextPageActionId = Math.Clamp(nextPageActionId, 0, 1000);
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("ReceiveEvent first int value used only for optional Next-page capture filtering (observed: 22).");
        }

        var actionInterval = _configuration.AutoDetailScanActionIntervalMs;
        if (ImGui.SliderInt("Action Interval", ref actionInterval, 100, 2000, "%d ms"))
        {
            _configuration.AutoDetailScanActionIntervalMs = actionInterval;
            _configuration.Save();
        }

        var detailTimeout = _configuration.AutoDetailScanDetailTimeoutMs;
        if (ImGui.SliderInt("Detail Timeout", ref detailTimeout, 500, 10000, "%d ms"))
        {
            _configuration.AutoDetailScanDetailTimeoutMs = detailTimeout;
            _configuration.Save();
        }

        var minDwell = _configuration.AutoDetailScanMinDwellMs;
        if (ImGui.SliderInt("Min Detail Dwell", ref minDwell, 100, 3000, "%d ms"))
        {
            _configuration.AutoDetailScanMinDwellMs = minDwell;
            _configuration.Save();
        }

        var postCooldown = _configuration.AutoDetailScanPostListingCooldownMs;
        if (ImGui.SliderInt("Post Listing Cooldown", ref postCooldown, 50, 3000, "%d ms"))
        {
            _configuration.AutoDetailScanPostListingCooldownMs = postCooldown;
            _configuration.Save();
        }

        var refreshInterval = _configuration.AutoDetailScanRefreshIntervalMs;
        if (ImGui.SliderInt("Refresh Interval", ref refreshInterval, 1000, 30000, "%d ms"))
        {
            _configuration.AutoDetailScanRefreshIntervalMs = refreshInterval;
            _configuration.Save();
        }

        var dedupTtl = _configuration.AutoDetailScanDedupTtlSeconds;
        if (ImGui.SliderInt("Dedup TTL", ref dedupTtl, 30, 3600, "%d s"))
        {
            _configuration.AutoDetailScanDedupTtlSeconds = dedupTtl;
            _configuration.Save();
        }

        var maxFailures = _configuration.AutoDetailScanMaxConsecutiveFailures;
        if (ImGui.SliderInt("Max Consecutive Failures", ref maxFailures, 1, 20, "%d"))
        {
            _configuration.AutoDetailScanMaxConsecutiveFailures = maxFailures;
            _configuration.Save();
        }

        var maxPerRun = _configuration.AutoDetailScanMaxPerRun;
        if (ImGui.SliderInt("Max Listings Per Run (0=unlimited)", ref maxPerRun, 0, 500, "%d"))
        {
            _configuration.AutoDetailScanMaxPerRun = maxPerRun;
            _configuration.Save();
        }

        ImGui.TextUnformatted($"State: {_plugin.DebugPfScanner.StateName}");
        ImGui.TextUnformatted($"Target Listing: {_plugin.DebugPfScanner.CurrentTargetListingId}");
        ImGui.TextUnformatted($"Visible Cache: {_plugin.DebugPfScanner.VisibleListingCount} / Pending: {_plugin.DebugPfScanner.PendingCount}");
        ImGui.TextUnformatted($"Processed: {_plugin.DebugPfScanner.ProcessedCount} / Consecutive Failures: {_plugin.DebugPfScanner.ConsecutiveFailures}");
        ImGui.TextUnformatted($"Last Attempt: listing={_plugin.DebugPfScanner.LastAttemptListingId} success={_plugin.DebugPfScanner.LastAttemptSuccess} reason={_plugin.DebugPfScanner.LastAttemptReason}");
        ImGui.TextUnformatted($"Gatherer Ack Version: {_plugin.Gatherer.LastSuccessfulUploadAckVersion} (indexed: {_plugin.Gatherer.UploadedListingIndexCount})");
        ImGui.TextUnformatted($"Gatherer Last Success UTC: {_plugin.Gatherer.LastSuccessfulUploadAtUtc:HH:mm:ss}");
        ImGui.TextUnformatted($"Detail Queue Ack Version: {_plugin.PartyDetailCollector.LastQueuedAckVersion} listing={_plugin.PartyDetailCollector.LastUploadedListingId}");
        ImGui.TextUnformatted($"Detail Ack Version: {_plugin.PartyDetailCollector.LastSuccessfulUploadAckVersion} listing={_plugin.PartyDetailCollector.LastSuccessfulUploadListingId}");
        ImGui.TextUnformatted($"Detail Last Success UTC: {_plugin.PartyDetailCollector.LastSuccessfulUploadAtUtc:HH:mm:ss}");
        ImGui.TextUnformatted($"Detail Missing Ack Version: {_plugin.PartyDetailCollector.LastTerminalUploadAckVersion} listing={_plugin.PartyDetailCollector.LastTerminalUploadListingId}");
        ImGui.TextUnformatted($"Detail Pending Queue: {_plugin.PartyDetailCollector.PendingQueueCount}");
        ImGui.TextUnformatted($"Next Page Button Capture (optional override): {(_plugin.DebugPfScanner.HasNextPageCapture ? "READY" : "MISSING")} armed={_plugin.DebugPfScanner.IsNextPageCaptureArmed}");
        if (_plugin.DebugPfScanner.HasNextPageCapture)
        {
            ImGui.TextUnformatted($"Captured Event: action={_plugin.DebugPfScanner.CapturedNextPageActionId} button={_plugin.DebugPfScanner.CapturedNextPageButtonId} kind={_plugin.DebugPfScanner.CapturedNextPageEventKind} values={_plugin.DebugPfScanner.CapturedNextPageValueCount} with_result={_plugin.DebugPfScanner.CapturedNextPageUsesWithResult}");
            ImGui.TextWrapped($"Captured Payload: {_plugin.DebugPfScanner.CapturedNextPageValues}");
        }
        ImGui.TextWrapped($"Last Observed ReceiveEvent: {_plugin.DebugPfScanner.LastObservedReceiveEvent}");
        ImGui.TextWrapped($"Last Observed Addon ReceiveEvent: {_plugin.DebugPfScanner.LastObservedAddonReceiveEvent}");

        if (ImGui.Button("Arm Next Page Capture"))
        {
            _plugin.DebugPfScanner.ArmNextPageCapture();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Optional: arm capture and click PF Next once to detect a runtime button id override. Normal scanning no longer requires this step.");
        }

        ImGui.SameLine();
        if (ImGui.Button("Clear Next Page Capture"))
        {
            _plugin.DebugPfScanner.ClearNextPageCapture();
        }

        if (ImGui.Button("Reset Scanner Session"))
        {
            _plugin.DebugPfScanner.ResetSession();
        }

        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.TextColored(new Vector4(0.4f, 1.0f, 0.4f, 1.0f), "[Player Cache Manual Sync]");
        ImGui.TextUnformatted($"Pending Player Cache Rows: {_plugin.PendingPlayerUploadCount}");
        ImGui.TextUnformatted($"Player Upload Worker: {(_plugin.IsPlayerUploadInProgress ? "BUSY" : "IDLE")}");

        if (ImGui.Button("Upload Pending Players Now"))
        {
            _plugin.TriggerPlayerUploadNow();
            _playerManualUploadStatus = $"Triggered pending upload at {DateTime.UtcNow:HH:mm:ss} UTC";
        }

        ImGui.SameLine();
        if (ImGui.Button("Requeue All Cached Players + Upload"))
        {
            var requeued = _plugin.TriggerPlayerFullResyncUploadNow();
            _playerManualUploadStatus = $"Requeued {requeued} cached rows and triggered upload at {DateTime.UtcNow:HH:mm:ss} UTC";
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("Marks every row in player_cache.db as dirty, then starts upload immediately.");
        }

        ImGui.TextWrapped("Chat commands: /rpr players-upload, /rpr players-resync");
        if (!string.IsNullOrEmpty(_playerManualUploadStatus))
        {
            ImGui.TextWrapped(_playerManualUploadStatus);
        }

        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.Text("Endpoint Status:");

        if (ImGui.BeginTable("cbStatusTable", 4, ImGuiTableFlags.Borders | ImGuiTableFlags.RowBg | ImGuiTableFlags.Resizable))
        {
            ImGui.TableSetupColumn("URL", ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn("Failures", ImGuiTableColumnFlags.WidthFixed, 60);
            ImGui.TableSetupColumn("Status", ImGuiTableColumnFlags.WidthFixed, 80);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, 60);
            ImGui.TableHeadersRow();

            foreach (var url in _configuration.UploadUrls)
            {
                ImGui.TableNextRow();
                
                ImGui.TableSetColumnIndex(0);
                ImGui.Text(url.Url);

                ImGui.TableSetColumnIndex(1);
                ImGui.Text($"{url.FailureCount} / {_configuration.CircuitBreakerFailureThreshold}");

                ImGui.TableSetColumnIndex(2);
                bool isOpen = url.FailureCount >= _configuration.CircuitBreakerFailureThreshold;
                if (isOpen)
                {
                    // Check if waiting period is over
                    var timeLeft = TimeSpan.FromMinutes(_configuration.CircuitBreakerBreakDurationMinutes) - (DateTime.UtcNow - url.LastFailureTime);
                    if (timeLeft.TotalSeconds > 0)
                    {
                        ImGui.TextColored(new Vector4(1, 0, 0, 1), $"OPEN ({timeLeft.TotalSeconds:F0}s)");
                    }
                    else
                    {
                        ImGui.TextColored(new Vector4(1, 1, 0, 1), "HALF-OPEN");
                    }
                }
                else
                {
                    ImGui.TextColored(new Vector4(0, 1, 0, 1), "CLOSED");
                }

                ImGui.TableSetColumnIndex(3);
                if (ImGui.Button($"Reset##{url.GetHashCode()}"))
                {
                    url.FailureCount = 0;
                    url.LastFailureTime = DateTime.MinValue;
                }
            }
            ImGui.EndTable();
        }
    }

    private void ResetToDefault()
    {
        _configuration.UploadUrls = Configuration.DefaultUploadUrls();
        _configuration.Save();
    }

    private static bool ValidUrl(string url)
        => Uri.TryCreate(url, UriKind.Absolute, out var uriResult)
           && (uriResult.Scheme == Uri.UriSchemeHttps || uriResult.Scheme == Uri.UriSchemeHttp);

    private static void DrawHelp(string text)
    {
        ImGui.SameLine();
        ImGuiComponents.HelpMarker(text);
    }

    private static void CopyToClipboard(string text)
    {
        try
        {
            ImGui.SetClipboardText(text);
        }
        catch (Exception)
        {
            // Ignore
        }
    }
}
