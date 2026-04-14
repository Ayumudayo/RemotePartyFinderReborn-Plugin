using System;
using System.Numerics;
using Dalamud.Bindings.ImGui;

namespace RemotePartyFinder;

internal sealed class DebugTabRenderer
{
    private readonly Plugin _plugin;
    private readonly Configuration _configuration;
    private string _scannerCollectionStatus = string.Empty;

    public DebugTabRenderer(Plugin plugin)
    {
        _plugin = plugin;
        _configuration = plugin.Configuration;
    }

    public void Draw()
    {
        DrawPfDetailAutoScannerSection();
    }

    private void DrawPfDetailAutoScannerSection()
    {
        ImGui.TextColored(new Vector4(0.4f, 1.0f, 0.4f, 1.0f), "[PF Detail Auto Scanner (Debug)]");
        ImGui.Spacing();

        ImGui.TextWrapped("Scanner runs only from current-page snapshots or collected listings.");

        var manualCollectionEnabled = _configuration.EnableManualPageCollectionDebug;
        if (ImGui.Checkbox("Manual Page Collection (Deferred Batch)", ref manualCollectionEnabled))
        {
            _configuration.EnableManualPageCollectionDebug = manualCollectionEnabled;
            _configuration.Save();
        }
        if (ImGui.IsItemHovered())
        {
            ImGui.SetTooltip("When enabled, listings seen while you manually turn PF pages are collected for a later one-click batch detail run.");
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
        ImGui.TextUnformatted($"Gatherer Last Success (Local): {FormatLocalClock(_plugin.Gatherer.LastSuccessfulUploadAtUtc)}");
        ImGui.TextUnformatted($"Detail Queue Ack Version: {_plugin.PartyDetailCollector.LastQueuedAckVersion} listing={_plugin.PartyDetailCollector.LastUploadedListingId}");
        ImGui.TextUnformatted($"Detail Ack Version: {_plugin.PartyDetailCollector.LastSuccessfulUploadAckVersion} listing={_plugin.PartyDetailCollector.LastSuccessfulUploadListingId}");
        ImGui.TextUnformatted($"Detail Last Success (Local): {FormatLocalClock(_plugin.PartyDetailCollector.LastSuccessfulUploadAtUtc)}");
        ImGui.TextUnformatted($"Detail Missing Ack Version: {_plugin.PartyDetailCollector.LastTerminalUploadAckVersion} listing={_plugin.PartyDetailCollector.LastTerminalUploadListingId}");
        ImGui.TextUnformatted($"Detail Pending Queue: {_plugin.PartyDetailCollector.PendingQueueCount}");
        ImGui.TextUnformatted($"Collected Listings: {_plugin.DebugPfScanner.CollectedListingCount}");

        if (ImGui.Button("Collect Current Page Snapshot"))
        {
            var added = _plugin.DebugPfScanner.CollectCurrentPageSnapshot();
            _scannerCollectionStatus = $"[{DateTime.Now:HH:mm:ss}] collected current-page snapshot (+{added})";
        }

        ImGui.SameLine();
        if (ImGui.Button("Run Collected Batch Click"))
        {
            var ok = _plugin.DebugPfScanner.StartCollectedBatchRun(out var status);
            _scannerCollectionStatus = $"[{DateTime.Now:HH:mm:ss}] {(ok ? "OK" : "FAIL")} {status}";
        }

        ImGui.SameLine();
        if (ImGui.Button("Clear Collected"))
        {
            var removed = _plugin.DebugPfScanner.ClearCollectedListings();
            _scannerCollectionStatus = $"[{DateTime.Now:HH:mm:ss}] cleared collected listings ({removed})";
        }

        if (!string.IsNullOrEmpty(_scannerCollectionStatus))
        {
            ImGui.TextWrapped(_scannerCollectionStatus);
        }

        if (ImGui.Button("Reset Scanner Session"))
        {
            _configuration.EnableAutoDetailScanDebug = false;
            _configuration.Save();
            _plugin.DebugPfScanner.ResetSession();
            _scannerCollectionStatus = $"[{DateTime.Now:HH:mm:ss}] scanner stopped and session reset";
        }
    }

    private static string FormatLocalClock(DateTime utcTime)
    {
        if (utcTime == DateTime.MinValue)
        {
            return "-";
        }

        var normalizedUtc = utcTime.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(utcTime, DateTimeKind.Utc)
            : utcTime;
        return normalizedUtc.ToLocalTime().ToString("HH:mm:ss");
    }
}
