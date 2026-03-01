using Dalamud.IoC;
using Dalamud.Game.Command;
using Dalamud.Interface.Windowing;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;
using ECommons;

namespace RemotePartyFinder;

public class Plugin : IDalamudPlugin {
    [PluginService]
    internal static IPluginLog Log { get; private set; }
    [PluginService]
    internal static IDalamudPluginInterface PluginInterface { get; private set; }

    [PluginService]
    internal IFramework Framework { get; private init; }

    [PluginService]
    internal IPartyFinderGui PartyFinderGui { get; private init; }

    [PluginService]
    internal IObjectTable ObjectTable { get; private init; }

    [PluginService]
    internal IAgentLifecycle AgentLifecycle { get; private init; }

    [PluginService]
    internal IAddonLifecycle AddonLifecycle { get; private init; }

    [PluginService]
    internal IGameGui GameGui { get; private init; }

    [PluginService]
    internal ICommandManager CommandManager { get; private init; }

    public Configuration Configuration { get; init; }
    public readonly WindowSystem WindowSystem = new("Remote Party Finder Reborn");
    private ConfigWindow ConfigWindow { get; init; }

    internal Gatherer Gatherer { get; }
    private PlayerCollector PlayerCollector { get; }
    internal PartyDetailCollector PartyDetailCollector { get; }
    internal DebugPfScanner DebugPfScanner { get; }
    private FFLogsCollector FFLogsCollector { get; }

    internal int PendingPlayerUploadCount => this.PlayerCollector.PendingCount;
    internal bool IsPlayerUploadInProgress => this.PlayerCollector.IsUploadInProgress;

    public Plugin() {
        ECommonsMain.Init(PluginInterface, this);

        Configuration = PluginInterface.GetPluginConfig() as Configuration ?? new Configuration();
        this.Gatherer = new Gatherer(this);
        this.PlayerCollector = new PlayerCollector(this);
        this.PartyDetailCollector = new PartyDetailCollector(this);
        this.DebugPfScanner = new DebugPfScanner(this, this.PartyDetailCollector, this.Gatherer);
        this.FFLogsCollector = new FFLogsCollector(this);
        ConfigWindow = new ConfigWindow(this);
        WindowSystem.AddWindow(ConfigWindow);
        PluginInterface.UiBuilder.Draw += DrawUI;
        PluginInterface.UiBuilder.OpenConfigUi += ToggleConfigUI;

        CommandManager.AddHandler("/rpr", new CommandInfo(OnCommand) {
            HelpMessage = "Open config UI. Args: players-upload, players-resync"
        });
    }

    public void Dispose() {
        this.Gatherer.Dispose();
        this.PlayerCollector.Dispose();
        this.PartyDetailCollector.Dispose();
        this.DebugPfScanner.Dispose();
        this.FFLogsCollector.Dispose();
        WindowSystem.RemoveAllWindows();
        ConfigWindow.Dispose();
        CommandManager.RemoveHandler("/rpr");

        ECommonsMain.Dispose();
    }

    private void OnCommand(string command, string args) {
        var normalizedArgs = (args ?? string.Empty).Trim().ToLowerInvariant();
        if (normalizedArgs == "players-upload") {
            TriggerPlayerUploadNow();
            return;
        }

        if (normalizedArgs == "players-resync") {
            _ = TriggerPlayerFullResyncUploadNow();
            return;
        }

        ToggleConfigUI();
    }

    internal void TriggerPlayerUploadNow() {
        this.PlayerCollector.TriggerManualUploadNow();
    }

    internal int TriggerPlayerFullResyncUploadNow() {
        return this.PlayerCollector.TriggerManualFullResyncUploadNow();
    }

    public void DrawUI() => WindowSystem.Draw();

    public void ToggleConfigUI() => ConfigWindow.Toggle();
}
