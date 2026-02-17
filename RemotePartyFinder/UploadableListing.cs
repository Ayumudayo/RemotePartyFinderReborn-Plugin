using System;
using System.Collections.Generic;
using System.Linq;
using Dalamud.Game.Gui.PartyFinder.Types;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class UploadableListing {
    public uint Id { get; }
    public uint ContentIdLower { get; }
    public byte[] Name { get; }
    public byte[] Description { get; }
    public ushort CreatedWorld { get; }
    public ushort HomeWorld { get; }
    public ushort CurrentWorld { get; }
    public DutyCategory Category { get; }
    public ushort Duty { get; }
    public DutyType DutyType { get; }
    public bool BeginnersWelcome { get; }
    public ushort SecondsRemaining { get; }
    public ushort MinItemLevel { get; }
    public byte NumParties { get; }
    public byte SlotsAvailable { get; }
    public uint LastServerRestart { get; }
    public ObjectiveFlags Objective { get; }
    public ConditionFlags Conditions { get; }
    public DutyFinderSettingsFlags DutyFinderSettings { get; }
    public LootRuleFlags LootRules { get; }
    public SearchAreaFlags SearchArea { get; }
    public List<UploadableSlot> Slots { get; }
    public List<byte> JobsPresent { get; }

    internal UploadableListing(IPartyFinderListing source) {
        Id = source.Id;
        ContentIdLower = (uint)source.ContentId;
        Name = source.Name.Encode();
        Description = source.Description.Encode();
        CreatedWorld = (ushort)source.World.Value.RowId;
        HomeWorld = (ushort)source.HomeWorld.Value.RowId;
        CurrentWorld = (ushort)source.CurrentWorld.Value.RowId;
        Category = source.Category;
        Duty = source.RawDuty;
        DutyType = source.DutyType;
        BeginnersWelcome = source.BeginnersWelcome;
        SecondsRemaining = source.SecondsRemaining;
        MinItemLevel = source.MinimumItemLevel;
        NumParties = source.Parties;
        SlotsAvailable = source.SlotsAvailable;
        LastServerRestart = source.LastPatchHotfixTimestamp;
        Objective = source.Objective;
        Conditions = source.Conditions;
        DutyFinderSettings = source.DutyFinderSettings;
        LootRules = source.LootRules;
        SearchArea = source.SearchArea;
        Slots = source.Slots.Select(static slot => new UploadableSlot(slot)).ToList();
        JobsPresent = source.RawJobsPresent.ToList();
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal sealed class UploadableSlot {
    public uint Accepting { get; }

    internal UploadableSlot(PartyFinderSlot source) {
        var mask = 0u;
        foreach (var acceptingFlag in source.Accepting) {
            mask |= (uint)acceptingFlag;
        }

        Accepting = mask;
    }
}
