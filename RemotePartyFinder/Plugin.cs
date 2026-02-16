using Dalamud.IoC;
using Dalamud.Game.Command;
using Dalamud.Interface.Windowing;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;

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
    internal IGameGui GameGui { get; private init; }

    [PluginService]
    internal ICommandManager CommandManager { get; private init; }

    public Configuration Configuration { get; init; }
    public readonly WindowSystem WindowSystem = new("Report Partry Finder Reborn");
    private ConfigWindow ConfigWindow { get; init; }

    private Gatherer Gatherer { get; }
    private PlayerCollector PlayerCollector { get; }
    private PartyDetailCollector PartyDetailCollector { get; }
    private FFLogsCollector FFLogsCollector { get; }

    public Plugin() {
        Configuration = PluginInterface.GetPluginConfig() as Configuration ?? new Configuration();
        this.Gatherer = new Gatherer(this);
        this.PlayerCollector = new PlayerCollector(this);
        this.PartyDetailCollector = new PartyDetailCollector(this);
        this.FFLogsCollector = new FFLogsCollector(this);
        ConfigWindow = new ConfigWindow(this);
        WindowSystem.AddWindow(ConfigWindow);
        PluginInterface.UiBuilder.Draw += DrawUI;
        PluginInterface.UiBuilder.OpenConfigUi += ToggleConfigUI;

        CommandManager.AddHandler("/rpr", new CommandInfo(OnCommand) {
            HelpMessage = "Open the configuration window."
        });
    }

    public void Dispose() {
        this.Gatherer.Dispose();
        this.PlayerCollector.Dispose();
        this.PartyDetailCollector.Dispose();
        this.FFLogsCollector.Dispose();
        WindowSystem.RemoveAllWindows();
        ConfigWindow.Dispose();
        CommandManager.RemoveHandler("/rpr");
    }

    private void OnCommand(string command, string args) {
        ToggleConfigUI();
    }

    public void DrawUI() => WindowSystem.Draw();

    public void ToggleConfigUI() => ConfigWindow.Toggle();
}
