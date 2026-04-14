using System;
using System.Numerics;
using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;

namespace RemotePartyFinder;

public class ConfigWindow : Window, IDisposable
{
    private readonly Configuration _configuration;
    private readonly SettingsTabRenderer _settingsTabRenderer;
    private readonly DebugTabRenderer _debugTabRenderer;

    public ConfigWindow(Plugin plugin) : base("Remote Party Finder Reborn")
    {
        _configuration = plugin.Configuration;
        _settingsTabRenderer = new SettingsTabRenderer(plugin);
        _debugTabRenderer = new DebugTabRenderer(plugin);
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
                _settingsTabRenderer.Draw();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem("Debug"))
            {
                _debugTabRenderer.Draw();
                ImGui.EndTabItem();
            }

            ImGui.EndTabBar();
        }
    }
}
