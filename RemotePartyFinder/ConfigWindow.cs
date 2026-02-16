using System;
using System.Collections.Generic;
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
    private readonly Configuration _configuration;
    private string _uploadUrlTempString = string.Empty;
    private string _uploadUrlError = string.Empty;

    public ConfigWindow(Plugin plugin) : base("Report Partry Finder Reborn")
    {
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
