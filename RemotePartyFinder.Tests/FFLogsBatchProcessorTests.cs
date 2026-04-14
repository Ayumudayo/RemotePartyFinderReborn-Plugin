using Xunit;

namespace RemotePartyFinder.Tests;

public sealed class FFLogsBatchProcessorTests
{
    static FFLogsBatchProcessorTests()
    {
        FFLogsTestAssemblyResolver.Register();
    }

    [Fact]
    public async Task Batch_processor_selects_the_best_candidate_per_content_id()
    {
        var apiClient = new StubFFLogsApiClient
        {
            OnFetchCharacterCandidateDataBatchAsync = static (queries, _, _, _, _, _) =>
            {
                var output = new Dictionary<string, FFLogsClient.CharacterFetchedData>();
                foreach (var query in queries)
                {
                    output[query.Key] = query.Server switch
                    {
                        "Alpha" => new FFLogsClient.CharacterFetchedData
                        {
                            Hidden = false,
                            Parses =
                            [
                                new FFLogsClient.CharacterEncounterParse
                                {
                                    EncounterId = 999,
                                    Percentile = 80.1,
                                    ClearCount = 2,
                                },
                            ],
                            RecentReportCodes = ["ALPHA"],
                        },
                        "Beta" => new FFLogsClient.CharacterFetchedData
                        {
                            Hidden = false,
                            Parses =
                            [
                                new FFLogsClient.CharacterEncounterParse
                                {
                                    EncounterId = 88,
                                    Percentile = 97.3,
                                    ClearCount = 5,
                                },
                            ],
                            RecentReportCodes = ["BETA"],
                        },
                        _ => new FFLogsClient.CharacterFetchedData(),
                    };
                }

                return Task.FromResult(output);
            },
        };
        var processor = CreateProcessor(apiClient);
        var session = new FFLogsLeaseSession(
            new UploadUrl("https://session-owner.example/"),
            [
                CreateJob(
                    contentId: 1001,
                    server: "Alpha",
                    candidateServers:
                    [
                        new ParseJobCandidateServer { Server = "Alpha", Region = "JP" },
                        new ParseJobCandidateServer { Server = "Beta", Region = "JP" },
                    ])
            ]);

        var result = await processor.ProcessLeaseSessionAsync(session, CancellationToken.None);

        var parse = Assert.Single(result.ProcessedResults);
        Assert.Equal("Beta", parse.MatchedServer);
        Assert.True(parse.IsEstimated);
        Assert.False(parse.IsHidden);
        Assert.Equal(97.3, parse.Encounters[88], 3);
        Assert.Equal(5, parse.ClearCounts[88]);
        Assert.False(result.HitRateLimitCooldown);
        Assert.False(result.ShouldAbandonRemainingLeases);
    }

    [Fact]
    public async Task Batch_processor_prefers_hidden_candidate_with_real_parses_over_visible_candidate_with_none()
    {
        var apiClient = new StubFFLogsApiClient
        {
            OnFetchCharacterCandidateDataBatchAsync = static (queries, _, _, _, _, _) =>
            {
                var output = new Dictionary<string, FFLogsClient.CharacterFetchedData>();
                foreach (var query in queries)
                {
                    output[query.Key] = query.Server switch
                    {
                        "VisibleEmpty" => new FFLogsClient.CharacterFetchedData
                        {
                            Hidden = false,
                            Parses = [],
                            RecentReportCodes = [],
                        },
                        "HiddenParsed" => new FFLogsClient.CharacterFetchedData
                        {
                            Hidden = true,
                            Parses =
                            [
                                new FFLogsClient.CharacterEncounterParse
                                {
                                    EncounterId = 88,
                                    Percentile = 75.0,
                                    ClearCount = 1,
                                },
                            ],
                            RecentReportCodes = ["HIDDEN-REPORT"],
                        },
                        _ => new FFLogsClient.CharacterFetchedData(),
                    };
                }

                return Task.FromResult(output);
            },
        };
        var processor = CreateProcessor(apiClient);
        var session = new FFLogsLeaseSession(
            new UploadUrl("https://session-owner.example/"),
            [
                CreateJob(
                    contentId: 1501,
                    server: "VisibleEmpty",
                    candidateServers:
                    [
                        new ParseJobCandidateServer { Server = "VisibleEmpty", Region = "JP" },
                        new ParseJobCandidateServer { Server = "HiddenParsed", Region = "JP" },
                    ])
            ]);

        var result = await processor.ProcessLeaseSessionAsync(session, CancellationToken.None);

        var parse = Assert.Single(result.ProcessedResults);
        Assert.Equal("HiddenParsed", parse.MatchedServer);
        Assert.True(parse.IsHidden);
    }

    [Fact]
    public async Task Batch_processor_preserves_hidden_parse_results_without_progress_enrichment()
    {
        var progressCalls = 0;
        var apiClient = new StubFFLogsApiClient
        {
            OnFetchCharacterCandidateDataBatchAsync = static (queries, _, _, _, _, _) =>
            {
                return Task.FromResult(new Dictionary<string, FFLogsClient.CharacterFetchedData>
                {
                    [queries[0].Key] = new FFLogsClient.CharacterFetchedData
                    {
                        Hidden = true,
                        Parses =
                        [
                            new FFLogsClient.CharacterEncounterParse
                            {
                                EncounterId = 88,
                                Percentile = 99.9,
                                ClearCount = 9,
                            },
                        ],
                        RecentReportCodes = ["HIDDEN"],
                    },
                });
            },
            OnFetchBestBossPercentByReportAsync = (_, _, _, _) =>
            {
                progressCalls++;
                return Task.FromResult(new Dictionary<string, double>());
            },
        };
        var processor = CreateProcessor(apiClient);
        var session = new FFLogsLeaseSession(
            new UploadUrl("https://session-owner.example/"),
            [CreateJob(contentId: 2001, server: "Hidden")]);

        var result = await processor.ProcessLeaseSessionAsync(session, CancellationToken.None);

        var parse = Assert.Single(result.ProcessedResults);
        Assert.True(parse.IsHidden);
        Assert.Empty(parse.Encounters);
        Assert.Empty(parse.ClearCounts);
        Assert.Empty(parse.BossPercentages);
        Assert.Equal(0, progressCalls);
    }

    [Fact]
    public async Task Batch_processor_merges_recent_report_progress_for_needed_encounters()
    {
        var progressEncounterIds = new List<int>();
        var apiClient = new StubFFLogsApiClient
        {
            OnFetchCharacterCandidateDataBatchAsync = static (queries, _, _, _, _, _) =>
            {
                return Task.FromResult(new Dictionary<string, FFLogsClient.CharacterFetchedData>
                {
                    [queries[0].Key] = new FFLogsClient.CharacterFetchedData
                    {
                        Hidden = false,
                        Parses =
                        [
                            new FFLogsClient.CharacterEncounterParse
                            {
                                EncounterId = 88,
                                Percentile = 91.2,
                                ClearCount = 4,
                            },
                        ],
                        RecentReportCodes = ["REP1", "REP2", "REP3"],
                    },
                });
            },
            OnFetchBestBossPercentByReportAsync = (_, encounterId, _, _) =>
            {
                progressEncounterIds.Add(encounterId);
                return Task.FromResult(encounterId switch
                {
                    88 => new Dictionary<string, double>
                    {
                        ["REP1"] = 17.5,
                        ["REP2"] = 9.8,
                    },
                    99 => new Dictionary<string, double>
                    {
                        ["REP2"] = 43.2,
                        ["REP3"] = 12.4,
                    },
                    _ => new Dictionary<string, double>(),
                });
            },
        };
        var processor = CreateProcessor(apiClient);
        var session = new FFLogsLeaseSession(
            new UploadUrl("https://session-owner.example/"),
            [CreateJob(contentId: 3001, server: "Visible", secondaryEncounterId: 99)]);

        var result = await processor.ProcessLeaseSessionAsync(session, CancellationToken.None);

        var parse = Assert.Single(result.ProcessedResults);
        Assert.Equal([88, 99], progressEncounterIds.OrderBy(static value => value));
        Assert.Equal(9.8, parse.BossPercentages[88], 3);
        Assert.Equal(12.4, parse.BossPercentages[99], 3);
        Assert.Equal(91.2, parse.Encounters[88], 3);
    }

    [Fact]
    public async Task Batch_processor_reports_rate_limit_cooldown_without_owning_submit_requeue_state()
    {
        var rateLimitChecks = 0;
        var apiClient = new StubFFLogsApiClient
        {
            OnTryGetRateLimitRemaining = () =>
            {
                rateLimitChecks++;
                return rateLimitChecks >= 3
                    ? (true, TimeSpan.FromSeconds(42))
                    : (false, TimeSpan.Zero);
            },
            OnFetchCharacterCandidateDataBatchAsync = static (queries, _, _, _, _, _) =>
            {
                return Task.FromResult(new Dictionary<string, FFLogsClient.CharacterFetchedData>
                {
                    [queries[0].Key] = new FFLogsClient.CharacterFetchedData
                    {
                        Hidden = true,
                    },
                });
            },
        };
        var processor = CreateProcessor(apiClient);
        var session = new FFLogsLeaseSession(
            new UploadUrl("https://session-owner.example/"),
            [
                CreateJob(contentId: 4001, server: "First", zoneId: 77),
                CreateJob(contentId: 4002, server: "Second", zoneId: 78),
            ]);

        var result = await processor.ProcessLeaseSessionAsync(session, CancellationToken.None);

        var parse = Assert.Single(result.ProcessedResults);
        Assert.Equal(4001UL, parse.ContentId);
        Assert.True(result.HitRateLimitCooldown);
        Assert.True(result.ShouldAbandonRemainingLeases);
        Assert.Equal(TimeSpan.FromSeconds(42), result.CooldownRemaining);
        Assert.False(result.HadTransientFailure);
    }

    private static FFLogsBatchProcessor CreateProcessor(StubFFLogsApiClient apiClient)
    {
        var seams = FFLogsCollector.CreateSeams(
            new StubFFLogsIngestHttpSender(),
            apiClient,
            new ManualFFLogsTimeProvider());
        return new FFLogsBatchProcessor(seams);
    }

    private static ParseJob CreateJob(
        ulong contentId,
        string server,
        uint zoneId = 77,
        uint encounterId = 88,
        uint? secondaryEncounterId = null,
        List<ParseJobCandidateServer>? candidateServers = null)
    {
        return new ParseJob
        {
            ContentId = contentId,
            Name = $"Player-{contentId}",
            Server = server,
            Region = "JP",
            CandidateServers = candidateServers ?? [],
            ZoneId = zoneId,
            DifficultyId = 5,
            Partition = 1,
            EncounterId = encounterId,
            SecondaryEncounterId = secondaryEncounterId,
            LeaseToken = $"lease-{contentId}",
        };
    }
}
