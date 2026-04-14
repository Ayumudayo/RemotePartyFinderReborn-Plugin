using Microsoft.Data.Sqlite;
using System.Reflection;
using System.Threading;
using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class CharaCardResolverRuntimeTests {
    static CharaCardResolverRuntimeTests() {
        DalamudAssemblyResolver.Register();
    }

    [Fact]
    public void ResolverQueue_picks_only_uncached_ids() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);
        database.UpsertResolvedIdentity(new CharacterIdentitySnapshot(
            1001UL,
            "Cached Player",
            74,
            "Tonberry",
            new DateTime(2026, 4, 13, 0, 0, 0, DateTimeKind.Utc)
        ));

        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 79 ? "Omega" : null,
            () => new DateTime(2026, 4, 13, 1, 0, 0, DateTimeKind.Utc)
        );

        resolver.EnqueueMany([0UL, 1001UL, 2002UL, 2002UL]);
        resolver.Pump();

        Assert.Equal([2002UL], runtime.RequestedContentIds);
    }

    [Fact]
    public void ResolverQueue_marks_response_as_resolved() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 2, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([3003UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(3003UL, 74, "Resolved Player"));
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(3003UL));

        resolver.Pump();

        Assert.Equal(ResolveState.Resolved, resolver.GetResolveState(3003UL));
        Assert.True(database.TryGetIdentity(3003UL, out var snapshot));
        Assert.Equal(
            new CharacterIdentitySnapshot(3003UL, "Resolved Player", 74, "Tonberry", nowUtc),
            snapshot
        );
    }

    [Fact]
    public void Pump_logs_dispatched_and_persisted_identity_lifecycle_for_successful_resolution() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var debugLogs = new List<string>();
        var nowUtc = new DateTime(2026, 4, 13, 2, 30, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc,
            debugSink: debugLogs.Add
        );

        resolver.EnqueueMany([3131UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(3131UL, 74, "Lifecycle Player"));
        resolver.Pump();

        Assert.Contains(
            debugLogs,
            message => message.Contains("dispatched identity request", StringComparison.Ordinal)
                && message.Contains("contentId=3131", StringComparison.Ordinal)
        );
        Assert.Contains(
            debugLogs,
            message => message.Contains("persisted resolved identity", StringComparison.Ordinal)
                && message.Contains("contentId=3131", StringComparison.Ordinal)
                && message.Contains("Lifecycle Player", StringComparison.Ordinal)
                && message.Contains("homeWorld=74", StringComparison.Ordinal)
                && message.Contains("Tonberry", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Hook_failure_disables_enrichment_without_throwing() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var runtime = new FakeCharaCardResolverRuntime {
            ThrowOnInitialize = true,
        };

        CharaCardResolver? instance = null;
        var resolver = Record.Exception(() => instance = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => new DateTime(2026, 4, 13, 3, 0, 0, DateTimeKind.Utc)
        ));

        Assert.Null(resolver);
        using var resolverInstance = Assert.IsType<CharaCardResolver>(instance);

        resolverInstance.EnqueueMany([4004UL]);
        resolverInstance.Pump();

        Assert.False(resolverInstance.IsEnabled);
        Assert.Empty(runtime.RequestedContentIds);
    }

    [Fact]
    public void Packet_projection_extracts_content_name_and_world() {
        var observedAtUtc = new DateTime(2026, 4, 13, 4, 0, 0, DateTimeKind.Utc);

        var projected = CharaCardResolver.TryProjectIdentity(
            new CharaCardPacketModel(5005UL, 21, "Projection Test"),
            worldId => worldId == 21 ? "Ravana" : null,
            observedAtUtc,
            out var snapshot
        );

        Assert.True(projected);
        Assert.Equal(
            new CharacterIdentitySnapshot(5005UL, "Projection Test", 21, "Ravana", observedAtUtc),
            snapshot
        );
    }

    [Fact]
    public void Dispose_stops_future_pump_activity_and_releases_runtime_handles() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var runtime = new FakeCharaCardResolverRuntime();
        var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => new DateTime(2026, 4, 13, 5, 0, 0, DateTimeKind.Utc)
        );

        resolver.EnqueueMany([6006UL]);
        resolver.Dispose();
        resolver.Pump();

        Assert.True(runtime.IsDisposed);
        Assert.Empty(runtime.RequestedContentIds);
    }

    [Fact]
    public void Pump_logs_warning_when_runtime_request_cannot_be_dispatched() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var warnings = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime {
            TryRequestResult = false,
        };
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => new DateTime(2026, 4, 13, 5, 30, 0, DateTimeKind.Utc),
            warningSink: warnings.Add
        );

        resolver.EnqueueMany([6116UL]);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(6116UL));
        Assert.Contains(
            warnings,
            message => message.Contains("failed to dispatch identity request", StringComparison.Ordinal)
                && message.Contains("contentId=6116", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void ResolverQueue_times_out_accepted_request_and_retries_after_backoff_when_no_packet_arrives() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 6, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => "Tonberry",
            () => nowUtc,
            requestTimeout: TimeSpan.FromSeconds(5)
        );

        resolver.EnqueueMany([7007UL]);
        resolver.Pump();

        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(7007UL));
        Assert.Equal([7007UL], runtime.RequestedContentIds);

        nowUtc = nowUtc.AddSeconds(6);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(7007UL));
        Assert.Equal([7007UL], runtime.RequestedContentIds);

        nowUtc = nowUtc.AddSeconds(10);
        resolver.Pump();

        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(7007UL));
        Assert.Equal([7007UL, 7007UL], runtime.RequestedContentIds);
    }

    [Fact]
    public void Packet_receipt_is_buffered_until_pump_persists_identity() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var persisted = new List<CharacterIdentitySnapshot>();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => new DateTime(2026, 4, 13, 7, 0, 0, DateTimeKind.Utc),
            persistResolvedIdentity: snapshot => persisted.Add(snapshot)
        );

        resolver.EnqueueMany([8008UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(8008UL, 74, "Buffered Player"));

        Assert.Empty(persisted);
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(8008UL));

        resolver.Pump();

        Assert.Equal(
            [
                new CharacterIdentitySnapshot(
                    8008UL,
                    "Buffered Player",
                    74,
                    "Tonberry",
                    new DateTime(2026, 4, 13, 7, 0, 0, DateTimeKind.Utc))
            ],
            persisted
        );
        Assert.Equal(ResolveState.Resolved, resolver.GetResolveState(8008UL));
    }

    [Fact]
    public void Failed_request_does_not_create_partial_identity_or_upload_when_plate_only_policy_enabled() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var warnings = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime {
            TryRequestResult = false,
        };

        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            utcNow: () => new DateTime(2026, 4, 13, 7, 30, 0, DateTimeKind.Utc),
            warningSink: warnings.Add
        );

        resolver.EnqueueMany([9444UL]);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(9444UL));
        Assert.False(database.TryGetIdentity(9444UL, out _));

        resolver.Pump();

        Assert.False(database.TryGetIdentity(9444UL, out _));
        Assert.False(database.TryGetPartialIdentity(9444UL, out _));
        Assert.Empty(database.TakePendingIdentityUploads(10));
        Assert.Contains(
            warnings,
            message => message.Contains("failed to dispatch identity request", StringComparison.Ordinal)
                && message.Contains("contentId=9444", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Local_dispatch_failure_consumes_shared_terminal_retry_budget() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var runtime = new FakeCharaCardResolverRuntime {
            TryRequestResult = false,
        };
        var nowUtc = new DateTime(2026, 4, 13, 7, 40, 0, DateTimeKind.Utc);

        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            utcNow: () => nowUtc
        );

        resolver.EnqueueMany([9445UL]);
        resolver.Pump();
        nowUtc = nowUtc.AddSeconds(10);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedPermanent, resolver.GetResolveState(9445UL));
        Assert.Equal([9445UL, 9445UL], runtime.RequestedContentIds);

        nowUtc = nowUtc.AddMinutes(5);
        resolver.Pump();

        Assert.Equal([9445UL, 9445UL], runtime.RequestedContentIds);
    }

    [Fact]
    public void Resolver_gives_up_after_retry_cap_is_reached() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 7, 45, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();

        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            utcNow: () => nowUtc,
            requestTimeout: TimeSpan.FromSeconds(1)
        );

        resolver.EnqueueMany([9555UL]);
        resolver.Pump();
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(9555UL));

        nowUtc = nowUtc.AddSeconds(2);
        resolver.Pump();
        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(9555UL));

        nowUtc = nowUtc.AddSeconds(10);
        resolver.Pump();
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(9555UL));

        nowUtc = nowUtc.AddSeconds(2);
        resolver.Pump();

        Assert.Equal(ResolveState.FailedPermanent, resolver.GetResolveState(9555UL));
        Assert.Equal([9555UL, 9555UL], runtime.RequestedContentIds);

        nowUtc = nowUtc.AddMinutes(5);
        resolver.Pump();

        Assert.Equal([9555UL, 9555UL], runtime.RequestedContentIds);
    }

    [Fact]
    public void Persistence_failure_leaves_request_retryable_instead_of_resolved() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 8, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 21 ? "Ravana" : null,
            () => nowUtc,
            persistResolvedIdentity: _ => throw new InvalidOperationException("persist failed")
        );

        resolver.EnqueueMany([9009UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(9009UL, 21, "Retry Player"));

        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(9009UL));

        var exception = Record.Exception(() => resolver.Pump());

        Assert.Null(exception);
        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(9009UL));
        Assert.False(database.TryGetIdentity(9009UL, out _));

        nowUtc = nowUtc.AddSeconds(10);
        resolver.Pump();

        Assert.Equal([9009UL, 9009UL], runtime.RequestedContentIds);
        Assert.Equal(ResolveState.InFlight, resolver.GetResolveState(9009UL));
    }

    [Fact]
    public void Unsolicited_packet_is_ignored_without_warning_or_exception() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var warnings = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => new DateTime(2026, 4, 13, 9, 0, 0, DateTimeKind.Utc),
            warningSink: warnings.Add
        );

        bool shouldPropagateOriginal = true;
        var exception = Record.Exception(() => shouldPropagateOriginal = runtime.Deliver(new CharaCardPacketModel(123456UL, 74, "Manual Player")));

        Assert.Null(exception);
        Assert.Empty(warnings);
        Assert.True(shouldPropagateOriginal);
        Assert.Equal(ResolveState.Unknown, resolver.GetResolveState(123456UL));
        Assert.False(database.TryGetIdentity(123456UL, out _));
    }

    [Fact]
    public void Plate_unavailable_packet_is_swallowed_to_avoid_game_ui_side_effects() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => new DateTime(2026, 4, 13, 9, 30, 0, DateTimeKind.Utc)
        );

        resolver.EnqueueMany([2222UL]);
        resolver.Pump();

        var shouldPropagateOriginal = runtime.Deliver(new CharaCardPacketModel(
            2222UL,
            74,
            "Unavailable Plate",
            Version: 0
        ));

        Assert.False(shouldPropagateOriginal);
        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(2222UL));
        Assert.False(database.TryGetIdentity(2222UL, out _));
    }

    [Fact]
    public void Tracked_agent_packet_is_swallowed_to_block_ui_entrypoint() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var debugLogs = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => new DateTime(2026, 4, 13, 9, 33, 0, DateTimeKind.Utc),
            debugSink: debugLogs.Add
        );

        resolver.EnqueueMany([2266UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(2266UL, 74, "Agent Packet Player"));

        Assert.False(runtime.DeliverAgentPacket(new CharaCardPacketModel(2266UL, 74, "Agent Packet Player")));
        Assert.Contains(
            debugLogs,
            message => message.Contains("swallowed AgentCharaCard.OpenCharaCardForPacket", StringComparison.Ordinal)
                && message.Contains("contentId=2266", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Response_dispatcher_failure_is_handled_without_banner_helper_specific_runtime_hooks() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 9, 35, 0, DateTimeKind.Utc);
        var debugLogs = new List<string>();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc,
            debugSink: debugLogs.Add
        );

        resolver.EnqueueMany([2323UL]);
        resolver.Pump();
        Assert.False(runtime.DeliverResponseDispatcher(new BannerHelperResponseModel(0, 1)));

        Assert.Equal(ResolveState.FailedTransient, resolver.GetResolveState(2323UL));
        Assert.Contains(
            debugLogs,
            message => message.Contains("suppressed CharaCard update failure response", StringComparison.Ordinal)
                && message.Contains("contentId=2323", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Suppression_state_consumes_budgets_without_needing_the_full_resolver() {
        var observedAtUtc = new DateTime(2026, 4, 13, 9, 36, 0, DateTimeKind.Utc);
        var state = new PlateFailureSuppressionState(
            gameLogSuppressionBudget: 2,
            selectOkSuppressionBudget: 2,
            gameLogSuppressionWindow: TimeSpan.FromSeconds(3),
            selectOkSuppressionWindow: TimeSpan.FromSeconds(1)
        );

        state.TrackUiOpenBudget(9001UL);
        state.TrackSelectOkDialogBudget(9001UL);
        state.ArmFailureSuppression(observedAtUtc);

        Assert.True(state.TryConsumeSuppressedUiOpen(9001UL));
        Assert.True(state.TryConsumeSuppressedUiOpen(9001UL));
        Assert.False(state.TryConsumeSuppressedUiOpen(9001UL));

        Assert.True(state.TryConsumeSuppressedSelectOkDialog(9001UL));
        Assert.True(state.TryConsumeSuppressedSelectOkDialog(9001UL));
        Assert.False(state.TryConsumeSuppressedSelectOkDialog(9001UL));

        Assert.Equal(SuppressionConsumption.WindowBudget, state.GetGameUiSuppression(observedAtUtc, hasInFlightRequest: false));
        Assert.Equal(SuppressionConsumption.WindowBudget, state.GetGameUiSuppression(observedAtUtc, hasInFlightRequest: false));
        Assert.Equal(SuppressionConsumption.InFlightRequest, state.GetGameUiSuppression(observedAtUtc.AddSeconds(4), hasInFlightRequest: true));
        Assert.Equal(SuppressionConsumption.None, state.GetGameUiSuppression(observedAtUtc.AddSeconds(4), hasInFlightRequest: false));

        Assert.Equal(SuppressionConsumption.WindowBudget, state.GetSelectOkSuppression(observedAtUtc, hasInFlightRequest: false));
        Assert.Equal(SuppressionConsumption.WindowBudget, state.GetSelectOkSuppression(observedAtUtc, hasInFlightRequest: false));
        Assert.Equal(SuppressionConsumption.InFlightRequest, state.GetSelectOkSuppression(observedAtUtc.AddSeconds(2), hasInFlightRequest: true));
        Assert.Equal(SuppressionConsumption.None, state.GetSelectOkSuppression(observedAtUtc.AddSeconds(2), hasInFlightRequest: false));
    }

    [Fact]
    public void Response_dispatcher_failure_without_inflight_request_is_not_suppressed() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 9, 40, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        Assert.True(runtime.DeliverResponseDispatcher(new BannerHelperResponseModel(0, 1)));
    }

    [Fact]
    public void Resolver_registers_and_unregisters_select_ok_lifecycle_handlers() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var addonLifecycle = new FakeAddonLifecycle();
        var runtime = new FakeCharaCardResolverRuntime();

        using (var resolver = new CharaCardResolver(
                   database,
                   runtime,
                   worldId => worldId == 74 ? "Tonberry" : null,
                   () => new DateTime(2026, 4, 13, 9, 45, 0, DateTimeKind.Utc),
                   selectOkDialogSuppressionRuntime: addonLifecycle
               )) {
            Assert.Contains("SelectOk.PreSetup", addonLifecycle.RegisteredSources);
            Assert.Contains("SelectOk.PreRequestedUpdate", addonLifecycle.RegisteredSources);
            Assert.Contains("SelectOk.PreRefresh", addonLifecycle.RegisteredSources);
            Assert.Contains("SelectOk.PreOpen", addonLifecycle.RegisteredSources);
            Assert.Contains("SelectOk.PreShow", addonLifecycle.RegisteredSources);
            Assert.Contains("SelectOkTitle.PreSetup", addonLifecycle.RegisteredSources);
        }

        Assert.Equal(addonLifecycle.RegisteredSources.Count, addonLifecycle.UnregisteredSources.Count);
    }

    [Fact]
    public void Failed_tracked_packet_suppresses_select_ok_addon_lifecycle_events_within_window() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 9, 50, 0, DateTimeKind.Utc);
        var debugLogs = new List<string>();
        var addonLifecycle = new FakeAddonLifecycle();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc,
            debugSink: debugLogs.Add,
            selectOkDialogSuppressionRuntime: addonLifecycle
        );

        resolver.EnqueueMany([2525UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(2525UL, 0, string.Empty));

        Assert.True(addonLifecycle.Deliver("SelectOk.PreSetup"));
        Assert.Contains(
            debugLogs,
            message => message.Contains("suppressed SelectOk.PreSetup", StringComparison.Ordinal)
        );
        Assert.True(addonLifecycle.Deliver("Toast.Quest"));
        Assert.True(addonLifecycle.Deliver("ChatLog.5856"));
    }

    [Fact]
    public void Select_ok_suppression_window_expires_without_hiding_unrelated_dialogs() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 9, 55, 0, DateTimeKind.Utc);
        var addonLifecycle = new FakeAddonLifecycle();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc,
            selectOkDialogSuppressionRuntime: addonLifecycle
        );

        resolver.EnqueueMany([2626UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(2626UL, 0, string.Empty));

        nowUtc = nowUtc.AddSeconds(2);

        Assert.False(addonLifecycle.Deliver("SelectOk.PreSetup"));
    }

    [Fact]
    public void Failed_tracked_packet_should_swallow_final_select_ok_dialog_creation() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 9, 58, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([2727UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(2727UL, 74, "Unavailable Plate", Version: 0));

        Assert.False(runtime.DeliverSelectOkDialogRequest(new SelectOkDialogRequestModel(
            2727UL,
            0x3AF3,
            3,
            IsNotCreated: true,
            WasResetDueToFantasia: false
        )));
    }

    [Fact]
    public void Failed_tracked_request_should_swallow_select_ok_state_transition_before_window_opens() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 10, 0, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([2828UL]);
        resolver.Pump();
        Assert.False(runtime.DeliverResponseDispatcher(new BannerHelperResponseModel(0, 1)));

        Assert.False(runtime.DeliverSelectOkStateTransition(new SelectOkStateTransitionModel(
            2828UL,
            0,
            CanEdit: true,
            IsNotCreated: false,
            WasResetDueToFantasia: false
        )));
    }

    [Fact]
    public void Failed_tracked_request_should_swallow_known_plate_failure_game_ui_messages() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 10, 2, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([2929UL]);
        resolver.Pump();
        runtime.Deliver(new CharaCardPacketModel(2929UL, 74, "Unavailable Plate", Version: 0));

        Assert.False(runtime.DeliverSimpleGameUiMessage(new GameUiMessageModel(0x16E0)));
        Assert.False(runtime.DeliverParameterizedGameUiMessage(new GameUiMessageModel(0x16F6, 5, true)));
        Assert.True(runtime.DeliverSimpleGameUiMessage(new GameUiMessageModel(0x1234)));
    }

    [Fact]
    public void Dispatched_request_should_preemptively_suppress_known_plate_failure_game_ui_messages() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 10, 4, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([3030UL]);
        resolver.Pump();

        Assert.False(runtime.DeliverSimpleGameUiMessage(new GameUiMessageModel(0x16E0)));
        Assert.False(runtime.DeliverParameterizedGameUiMessage(new GameUiMessageModel(0x16F6, 5, true)));
    }

    [Fact]
    public void Inflight_request_should_still_suppress_known_plate_failure_messages_after_window_expires() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 10, 6, 0, DateTimeKind.Utc);
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc
        );

        resolver.EnqueueMany([3131UL]);
        resolver.Pump();

        nowUtc = nowUtc.AddSeconds(4);

        Assert.False(runtime.DeliverSimpleGameUiMessage(new GameUiMessageModel(0x16E0)));
        Assert.False(runtime.DeliverParameterizedGameUiMessage(new GameUiMessageModel(0x16F6, 5, true)));
    }

    [Fact]
    public void Inflight_request_should_still_suppress_select_ok_addon_events_after_window_expires() {
        using var harness = new TempPlayerCacheDatabase();
        using var database = new PlayerLocalDatabase(harness.DatabasePath);

        var nowUtc = new DateTime(2026, 4, 13, 10, 8, 0, DateTimeKind.Utc);
        var addonLifecycle = new FakeAddonLifecycle();
        var runtime = new FakeCharaCardResolverRuntime();
        using var resolver = new CharaCardResolver(
            database,
            runtime,
            worldId => worldId == 74 ? "Tonberry" : null,
            () => nowUtc,
            selectOkDialogSuppressionRuntime: addonLifecycle
        );

        resolver.EnqueueMany([3232UL]);
        resolver.Pump();

        nowUtc = nowUtc.AddSeconds(2);

        Assert.True(addonLifecycle.Deliver("SelectOk.PreSetup"));
    }

    private sealed class FakeCharaCardResolverRuntime : ICharaCardResolverRuntime {
        private Func<CharaCardPacketModel, bool>? _packetHandler;
        private Func<CharaCardPacketModel, bool>? _agentPacketHandler;
        private Func<BannerHelperResponseModel, bool>? _responseDispatcherHandler;
        private Func<SelectOkStateTransitionModel, bool>? _selectOkStateTransitionHandler;
        private Func<GameUiMessageModel, bool>? _simpleGameUiMessageHandler;
        private Func<GameUiMessageModel, bool>? _parameterizedGameUiMessageHandler;
        private Func<SelectOkDialogRequestModel, bool>? _selectOkDialogHandler;

        public List<ulong> RequestedContentIds { get; } = [];

        public ResolverPreflightResult PreflightResult { get; init; } = new(true, "Ready");

        public bool ThrowOnInitialize { get; init; }

        public bool TryRequestResult { get; init; } = true;

        public bool IsDisposed { get; private set; }

        public ResolverPreflightResult CheckAvailability() => PreflightResult;

        public void Initialize(
            Func<CharaCardPacketModel, bool> packetHandler,
            Func<CharaCardPacketModel, bool> agentPacketHandler,
            Func<BannerHelperResponseModel, bool> responseDispatcherHandler,
            Func<SelectOkStateTransitionModel, bool> selectOkStateTransitionHandler,
            Func<GameUiMessageModel, bool> simpleGameUiMessageHandler,
            Func<GameUiMessageModel, bool> parameterizedGameUiMessageHandler,
            Func<SelectOkDialogRequestModel, bool> selectOkDialogHandler
        ) {
            if (ThrowOnInitialize) {
                throw new InvalidOperationException("boom");
            }

            _packetHandler = packetHandler;
            _agentPacketHandler = agentPacketHandler;
            _responseDispatcherHandler = responseDispatcherHandler;
            _selectOkStateTransitionHandler = selectOkStateTransitionHandler;
            _simpleGameUiMessageHandler = simpleGameUiMessageHandler;
            _parameterizedGameUiMessageHandler = parameterizedGameUiMessageHandler;
            _selectOkDialogHandler = selectOkDialogHandler;
        }

        public bool TryRequest(ulong contentId) {
            RequestedContentIds.Add(contentId);
            return TryRequestResult;
        }

        public bool Deliver(CharaCardPacketModel packet) {
            return _packetHandler?.Invoke(packet) ?? true;
        }

        public bool DeliverAgentPacket(CharaCardPacketModel packet) {
            return _agentPacketHandler?.Invoke(packet) ?? true;
        }

        public bool DeliverResponseDispatcher(BannerHelperResponseModel response) {
            return !(_responseDispatcherHandler?.Invoke(response) ?? false);
        }

        public bool DeliverSelectOkDialogRequest(SelectOkDialogRequestModel request) {
            return _selectOkDialogHandler?.Invoke(request) ?? true;
        }

        public bool DeliverSelectOkStateTransition(SelectOkStateTransitionModel request) {
            return _selectOkStateTransitionHandler?.Invoke(request) ?? true;
        }

        public bool DeliverSimpleGameUiMessage(GameUiMessageModel message) {
            return _simpleGameUiMessageHandler?.Invoke(message) ?? true;
        }

        public bool DeliverParameterizedGameUiMessage(GameUiMessageModel message) {
            return _parameterizedGameUiMessageHandler?.Invoke(message) ?? true;
        }

        public void Dispose() {
            IsDisposed = true;
            _packetHandler = null;
            _agentPacketHandler = null;
            _responseDispatcherHandler = null;
            _selectOkStateTransitionHandler = null;
            _simpleGameUiMessageHandler = null;
            _parameterizedGameUiMessageHandler = null;
            _selectOkDialogHandler = null;
        }
    }

    private sealed class FakeAddonLifecycle : ISelectOkDialogSuppressionRuntime {
        private static readonly string[] KnownSources = [
            "SelectOk.PreSetup",
            "SelectOk.PreRequestedUpdate",
            "SelectOk.PreRefresh",
            "SelectOk.PreOpen",
            "SelectOk.PreShow",
            "SelectOkTitle.PreSetup",
            "SelectOkTitle.PreRequestedUpdate",
            "SelectOkTitle.PreRefresh",
            "SelectOkTitle.PreOpen",
            "SelectOkTitle.PreShow",
            "Toast.Normal",
            "Toast.Quest",
            "Toast.Error",
            "ChatLog.5856",
        ];

        private readonly List<string> _activeSources = [];
        private Func<string, bool>? _handler;

        public List<string> RegisteredSources { get; } = [];

        public List<string> UnregisteredSources { get; } = [];

        public void Initialize(Func<string, bool> selectOkHandler) {
            _handler = selectOkHandler;
            foreach (var source in KnownSources) {
                _activeSources.Add(source);
                RegisteredSources.Add(source);
            }
        }

        public bool Deliver(string source) {
            if (!_activeSources.Contains(source)) {
                return false;
            }

            return _handler?.Invoke(source) ?? false;
        }

        public void Dispose() {
            foreach (var source in _activeSources) {
                UnregisteredSources.Add(source);
            }

            _activeSources.Clear();
            _handler = null;
        }
    }

    private sealed class TempPlayerCacheDatabase : IDisposable {
        private readonly string _directoryPath;

        public TempPlayerCacheDatabase() {
            _directoryPath = Path.Combine(Path.GetTempPath(), "RemotePartyFinder.Tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, "player_cache.db");
        }

        public string DatabasePath { get; }

        public void Dispose() {
            SqliteConnection.ClearAllPools();

            if (Directory.Exists(_directoryPath)) {
                Directory.Delete(_directoryPath, recursive: true);
            }
        }
    }

    private static class DalamudAssemblyResolver {
        private static int _registered;

        public static void Register() {
            if (Interlocked.Exchange(ref _registered, 1) != 0) {
                return;
            }

            AppDomain.CurrentDomain.AssemblyResolve += static (_, args) => {
                var assemblyName = new AssemblyName(args.Name).Name;
                if (string.IsNullOrWhiteSpace(assemblyName)) {
                    return null;
                }

                var dalamudHome = Environment.GetEnvironmentVariable("DALAMUD_HOME");
                if (string.IsNullOrWhiteSpace(dalamudHome)) {
                    dalamudHome = Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                        "XIVLauncher",
                        "addon",
                        "Hooks",
                        "dev"
                    );
                }

                var candidatePath = Path.Combine(dalamudHome, assemblyName + ".dll");
                return File.Exists(candidatePath) ? Assembly.LoadFrom(candidatePath) : null;
            };
        }
    }
}
