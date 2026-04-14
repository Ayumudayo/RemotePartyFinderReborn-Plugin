using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Plugin.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

internal interface IFFLogsIngestHttpSender
{
    Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken);
}

internal interface IFFLogsApiClient
{
    bool TryGetRateLimitRemaining(out TimeSpan remaining);
    DateTime RateLimitCooldownUntilUtc { get; }
    void ResetRateLimitCooldown();
    Task<Dictionary<string, FFLogsClient.CharacterFetchedData>> FetchCharacterCandidateDataBatchAsync(
        List<FFLogsClient.CandidateCharacterQuery> queries,
        int zoneId,
        int? difficultyId,
        int? partition,
        int recentReportsLimit,
        CancellationToken cancellationToken);
    Task<Dictionary<string, double>> FetchBestBossPercentByReportAsync(
        List<string> reportCodes,
        int encounterId,
        int? difficultyId,
        CancellationToken cancellationToken);
}

internal interface IFFLogsTimeProvider
{
    DateTime UtcNow { get; }
}

internal sealed record FFLogsCollectorSeams(
    IFFLogsIngestHttpSender IngestHttpSender,
    IFFLogsApiClient ApiClient,
    IFFLogsTimeProvider TimeProvider);

public class FFLogsCollector : IDisposable
{
    private sealed class HttpClientIngestHttpSender(HttpClient httpClient) : IFFLogsIngestHttpSender
    {
        public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => httpClient.SendAsync(request, cancellationToken);
    }

    private sealed class FFLogsApiClientAdapter(FFLogsClient client) : IFFLogsApiClient
    {
        public bool TryGetRateLimitRemaining(out TimeSpan remaining)
            => client.TryGetRateLimitRemaining(out remaining);

        public DateTime RateLimitCooldownUntilUtc
            => client.RateLimitCooldownUntilUtc;

        public void ResetRateLimitCooldown()
            => client.ResetRateLimitCooldown();

        public Task<Dictionary<string, FFLogsClient.CharacterFetchedData>> FetchCharacterCandidateDataBatchAsync(
            List<FFLogsClient.CandidateCharacterQuery> queries,
            int zoneId,
            int? difficultyId,
            int? partition,
            int recentReportsLimit,
            CancellationToken cancellationToken)
            => client.FetchCharacterCandidateDataBatchAsync(
                queries,
                zoneId,
                difficultyId,
                partition,
                recentReportsLimit,
                cancellationToken);

        public Task<Dictionary<string, double>> FetchBestBossPercentByReportAsync(
            List<string> reportCodes,
            int encounterId,
            int? difficultyId,
            CancellationToken cancellationToken)
            => client.FetchBestBossPercentByReportAsync(
                reportCodes,
                encounterId,
                difficultyId,
                cancellationToken);
    }

    private sealed class SystemFFLogsTimeProvider : IFFLogsTimeProvider
    {
        public DateTime UtcNow
            => DateTime.UtcNow;
    }

    private Configuration Configuration { get; set; }
    private Action<IFramework.OnUpdateDelegate> SubscribeFrameworkUpdate { get; set; } = static _ => { };
    private Action<IFramework.OnUpdateDelegate> UnsubscribeFrameworkUpdate { get; set; } = static _ => { };
    private Action<string> InfoLog { get; set; } = static _ => { };
    private Action<string> WarningLog { get; set; } = static _ => { };
    private Action<string> ErrorLog { get; set; } = static _ => { };
    private Action<string> DebugLog { get; set; } = static _ => { };
    private FFLogsCollectorSeams Seams { get; set; }
    private FFLogsSubmitBuffer SubmitBuffer { get; set; }
    private FFLogsWorkerPolicy WorkerPolicy { get; set; }
    private FFLogsLeaseAbandoner LeaseAbandoner { get; set; }
    private FFLogsJobLeaseClient JobLeaseClient { get; set; }
    private IReadOnlyList<IDisposable> OwnedDisposables { get; set; } = [];
    private bool IsRunning { get; set; }
    private System.Threading.CancellationTokenSource Cts { get; set; } = new();

    internal static FFLogsCollectorSeams CreateSeams(
        IFFLogsIngestHttpSender ingestHttpSender,
        IFFLogsApiClient apiClient,
        IFFLogsTimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(ingestHttpSender);
        ArgumentNullException.ThrowIfNull(apiClient);
        ArgumentNullException.ThrowIfNull(timeProvider);
        return new FFLogsCollectorSeams(ingestHttpSender, apiClient, timeProvider);
    }

    internal static FFLogsCollector CreateForTesting(
        Configuration configuration,
        FFLogsCollectorSeams seams,
        FFLogsSubmitBuffer submitBuffer,
        FFLogsWorkerPolicy workerPolicy,
        FFLogsJobLeaseClient jobLeaseClient = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(seams);
        ArgumentNullException.ThrowIfNull(submitBuffer);
        ArgumentNullException.ThrowIfNull(workerPolicy);

        var collector = new FFLogsCollector();
        collector.Initialize(
            configuration,
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { },
            seams,
            submitBuffer,
            workerPolicy,
            new FFLogsLeaseAbandoner(seams),
            jobLeaseClient ?? new FFLogsJobLeaseClient(seams),
            [],
            startWorker: false);
        return collector;
    }

    internal Task RunWorkerLoopForTestingAsync()
        => WorkerLoop();

    private FFLogsCollector()
    {
    }

    public FFLogsCollector(Plugin plugin)
    {
        ArgumentNullException.ThrowIfNull(plugin);

        var httpClient = new HttpClient();
        var ffLogsClient = new FFLogsClient(plugin.Configuration);
        var seams = CreateSeams(
            new HttpClientIngestHttpSender(httpClient),
            new FFLogsApiClientAdapter(ffLogsClient),
            new SystemFFLogsTimeProvider());
        Initialize(
            plugin.Configuration,
            handler => plugin.Framework.Update += handler,
            handler => plugin.Framework.Update -= handler,
            message => Plugin.Log.Info(message),
            message => Plugin.Log.Warning(message),
            message => Plugin.Log.Error(message),
            message => Plugin.Log.Debug(message),
            seams,
            new FFLogsSubmitBuffer(),
            new FFLogsWorkerPolicy(plugin.Configuration, message => Plugin.Log.Warning(message), seams.TimeProvider),
            new FFLogsLeaseAbandoner(seams),
            new FFLogsJobLeaseClient(seams),
            [ffLogsClient, httpClient],
            startWorker: true);
    }

    public void Dispose()
    {
        UnsubscribeFrameworkUpdate(OnUpdate);
        Cts.Cancel();
        foreach (var disposable in OwnedDisposables)
        {
            disposable.Dispose();
        }
    }

    private void Initialize(
        Configuration configuration,
        Action<IFramework.OnUpdateDelegate> subscribeFrameworkUpdate,
        Action<IFramework.OnUpdateDelegate> unsubscribeFrameworkUpdate,
        Action<string> infoLog,
        Action<string> warningLog,
        Action<string> errorLog,
        Action<string> debugLog,
        FFLogsCollectorSeams seams,
        FFLogsSubmitBuffer submitBuffer,
        FFLogsWorkerPolicy workerPolicy,
        FFLogsLeaseAbandoner leaseAbandoner,
        FFLogsJobLeaseClient jobLeaseClient,
        IReadOnlyList<IDisposable> ownedDisposables,
        bool startWorker)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        SubscribeFrameworkUpdate = subscribeFrameworkUpdate ?? throw new ArgumentNullException(nameof(subscribeFrameworkUpdate));
        UnsubscribeFrameworkUpdate = unsubscribeFrameworkUpdate ?? throw new ArgumentNullException(nameof(unsubscribeFrameworkUpdate));
        InfoLog = infoLog ?? throw new ArgumentNullException(nameof(infoLog));
        WarningLog = warningLog ?? throw new ArgumentNullException(nameof(warningLog));
        ErrorLog = errorLog ?? throw new ArgumentNullException(nameof(errorLog));
        DebugLog = debugLog ?? throw new ArgumentNullException(nameof(debugLog));
        Seams = seams ?? throw new ArgumentNullException(nameof(seams));
        SubmitBuffer = submitBuffer ?? throw new ArgumentNullException(nameof(submitBuffer));
        WorkerPolicy = workerPolicy ?? throw new ArgumentNullException(nameof(workerPolicy));
        LeaseAbandoner = leaseAbandoner ?? throw new ArgumentNullException(nameof(leaseAbandoner));
        JobLeaseClient = jobLeaseClient ?? throw new ArgumentNullException(nameof(jobLeaseClient));
        OwnedDisposables = ownedDisposables ?? throw new ArgumentNullException(nameof(ownedDisposables));
        SubscribeFrameworkUpdate(OnUpdate);
        if (startWorker)
        {
            StartWorker();
        }
    }

    private void OnUpdate(IFramework framework)
    {
        // Placeholder if we need frame-perfect logic
    }

    private void StartWorker()
    {
        IsRunning = true;
        Task.Run(WorkerLoop, Cts.Token);
    }

    private static long ScoreCandidate(
        FFLogsClient.CharacterFetchedData data,
        uint encounterId,
        uint? secondaryEncounterId)
    {
        long score = 0;

        // Prefer candidates that have visible rankings/progress.
        if (!data.Hidden)
        {
            score += 1;
        }

        if (data.Parses.Count > 0)
        {
            score += 1000 + data.Parses.Count;
        }

        void ScoreEncounter(uint? enc)
        {
            if (!enc.HasValue || enc.Value == 0) return;
            var hit = data.Parses.FirstOrDefault(p => p.EncounterId == (int)enc.Value);
            if (hit == null) return;
            // Strong signal: this exact encounter has logs.
            score += 100_000 + (long)Math.Round(hit.Percentile * 100.0);
        }

        ScoreEncounter(encounterId);
        ScoreEncounter(secondaryEncounterId);

        score += Math.Min(data.RecentReportCodes.Count, 10);
        return score;
    }

    private static List<ParseJobCandidateServer> GetCandidates(ParseJob job)
    {
        if (job.CandidateServers != null && job.CandidateServers.Count > 0)
        {
            // Dedup while preserving order.
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = new List<ParseJobCandidateServer>();
            foreach (var c in job.CandidateServers)
            {
                var server = (c?.Server ?? "").Trim();
                var region = (c?.Region ?? "").Trim();
                if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(region))
                    continue;
                var key = server + "|" + region;
                if (!seen.Add(key))
                    continue;
                list.Add(new ParseJobCandidateServer { Server = server, Region = region });
            }
            return list;
        }

        if (!string.IsNullOrWhiteSpace(job.Server) && !string.IsNullOrWhiteSpace(job.Region))
        {
            return new List<ParseJobCandidateServer>
            {
                new() { Server = job.Server.Trim(), Region = job.Region.Trim() }
            };
        }

        return new List<ParseJobCandidateServer>();
    }

    private List<ParseResult> BuildSubmitBatch(List<ParseResult> freshResults)
        => SubmitBuffer.BuildSubmitBatch(freshResults);

    public bool TryGetRateLimitCooldownRemaining(out TimeSpan remaining)
        => Seams.ApiClient.TryGetRateLimitRemaining(out remaining);

    public DateTime RateLimitCooldownUntilUtc
        => Seams.ApiClient.RateLimitCooldownUntilUtc;

    public void ResetRateLimitCooldown()
        => Seams.ApiClient.ResetRateLimitCooldown();

    private static bool TryParseResultsSubmitResponse(string content, out ContributeFflogsResultsResponse parsed)
    {
        parsed = null;
        if (string.IsNullOrWhiteSpace(content))
        {
            return false;
        }

        try
        {
            parsed = JsonConvert.DeserializeObject<ContributeFflogsResultsResponse>(content);
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }

    private async Task WorkerLoop()
    {
        var consecutiveFailures = 0;
        while (!Cts.Token.IsCancellationRequested)
        {
            try
            {
                var hadTransientFailure = false;

                if (!Configuration.EnableFFLogsWorker)
                {
                    consecutiveFailures = 0;
                    await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                    continue;
                }

                if (string.IsNullOrEmpty(Configuration.FFLogsClientId) || 
                    string.IsNullOrEmpty(Configuration.FFLogsClientSecret))
                {
                    consecutiveFailures = 0;
                    await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                    continue;
                }

                if (Seams.ApiClient.TryGetRateLimitRemaining(out var rateLimitRemaining))
                {
                    consecutiveFailures = 0;
                    WorkerPolicy.LogCooldownSkipIfNeeded(rateLimitRemaining);
                    await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                    continue;
                }

                // 1. Request Work
                var leaseAttempt = await JobLeaseClient.TryAcquireSessionAsync(
                    Configuration,
                    Cts.Token,
                    WarningLog,
                    DebugLog);
                hadTransientFailure |= leaseAttempt.HadTransientFailure;
                var leaseSession = leaseAttempt.Session;

                if (leaseSession == null)
                {
                    consecutiveFailures = 0;
                    await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                    continue;
                }

                if (!leaseSession.HasJobs)
                {
                    if (hadTransientFailure)
                    {
                        consecutiveFailures = await WorkerPolicy.DelayWithBackoffAsync(consecutiveFailures, Cts.Token);
                    }
                    else if (leaseSession.UseBaseDelayWhenNoWork)
                    {
                        consecutiveFailures = 0;
                        await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                    }
                    else
                    {
                        consecutiveFailures = 0;
                        await WorkerPolicy.DelayAsync(WorkerPolicy.ComputeIdleDelayMs(), Cts.Token);
                    }
                    continue;
                }

                var jobs = leaseSession.Jobs;
                DebugLog($"Received {jobs.Count} players to fetch from FFLogs.");

                // 2. Fetch from FFLogs
                // Group by Zone (+ criteria)
                var results = new List<ParseResult>();
                var jobsByZone = jobs.GroupBy(j => new { j.ZoneId, j.DifficultyId, j.Partition });

                const int recentReportsLimit = 10;
                const int reportsToCheckForProgress = 5;
                var hitRateLimitCooldown = false;
                var cooldownRemaining = TimeSpan.Zero;

                foreach (var group in jobsByZone)
                {
                    if (Seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
                    {
                        hitRateLimitCooldown = true;
                        break;
                    }

                    var zoneId = group.Key.ZoneId;
                    var difficultyId = group.Key.DifficultyId == 0 ? (int?)null : group.Key.DifficultyId;
                    var partition = group.Key.Partition == 0 ? (int?)null : group.Key.Partition;

                    // Dedup characters by ContentId
                    var uniqueJobs = group
                        .GroupBy(j => j.ContentId)
                        .Select(g => g.First())
                        .ToList();

                    // Candidate probing (for unknown home worlds)
                    var candidatesByCid = new Dictionary<ulong, List<ParseJobCandidateServer>>();
                    var candidateQueries = new List<FFLogsClient.CandidateCharacterQuery>();

                    foreach (var job in uniqueJobs)
                    {
                        var candidates = GetCandidates(job);
                        if (candidates.Count == 0)
                            continue;

                        candidatesByCid[job.ContentId] = candidates;

                        for (var i = 0; i < candidates.Count; i++)
                        {
                            var c = candidates[i];
                            candidateQueries.Add(new FFLogsClient.CandidateCharacterQuery
                            {
                                Key = $"{job.ContentId}:{i}",
                                Name = job.Name,
                                Server = c.Server,
                                Region = c.Region,
                            });
                        }
                    }

                    var fetchedByKey = await Seams.ApiClient.FetchCharacterCandidateDataBatchAsync(
                        candidateQueries,
                        (int)zoneId,
                        difficultyId,
                        partition,
                        recentReportsLimit,
                        Cts.Token
                    );

                    if (Seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
                    {
                        hitRateLimitCooldown = true;
                        break;
                    }

                    // Create result objects per character (choose best candidate per ContentId)
                    var resultsByContentId = new Dictionary<ulong, ParseResult>();
                    var chosenDataByCid = new Dictionary<ulong, FFLogsClient.CharacterFetchedData>();

                    foreach (var job in uniqueJobs)
                    {
                        if (!candidatesByCid.TryGetValue(job.ContentId, out var candidates) || candidates.Count == 0)
                            continue;

                        var bestIdx = -1;
                        var bestScore = long.MinValue;
                        FFLogsClient.CharacterFetchedData bestData = null;

                        for (var i = 0; i < candidates.Count; i++)
                        {
                            var key = $"{job.ContentId}:{i}";
                            if (!fetchedByKey.TryGetValue(key, out var data))
                                continue;

                            var score = ScoreCandidate(data, job.EncounterId, job.SecondaryEncounterId);
                            if (bestIdx < 0 || score > bestScore)
                            {
                                bestIdx = i;
                                bestScore = score;
                                bestData = data;
                            }
                        }

                        if (bestIdx < 0 || bestData == null)
                        {
                            // 후보 데이터를 전혀 얻지 못한 경우(일시적 API 실패 등) - 이번 사이클에서는 스킵
                            continue;
                        }

                        var matched = candidates[bestIdx];
                        var pr = new ParseResult
                        {
                            ContentId = job.ContentId,
                            ZoneId = zoneId,
                            DifficultyId = group.Key.DifficultyId,
                            Partition = group.Key.Partition,
                            IsHidden = bestData.Hidden,
                            IsEstimated = job.CandidateServers != null && job.CandidateServers.Count > 0,
                            MatchedServer = matched.Server,
                            LeaseToken = job.LeaseToken,
                        };

                        if (!pr.IsHidden)
                        {
                            pr.Encounters = bestData.Parses
                                .GroupBy(e => e.EncounterId)
                                .ToDictionary(g => g.Key, g => g.Max(x => x.Percentile));

                            pr.ClearCounts = bestData.Parses
                                .Where(e => e.ClearCount.HasValue && e.ClearCount.Value > 0)
                                .GroupBy(e => e.EncounterId)
                                .ToDictionary(g => g.Key, g => g.Max(x => x.ClearCount!.Value));
                        }

                        resultsByContentId[job.ContentId] = pr;
                        chosenDataByCid[job.ContentId] = bestData;
                    }

                    // Progress: Boss remaining HP % (per encounter)
                    var nonHiddenJobs = uniqueJobs
                        .Where(j => resultsByContentId.TryGetValue(j.ContentId, out var r) && !r.IsHidden)
                        .ToList();

                    var encounterIdsNeeded = new HashSet<uint>();
                    foreach (var job in nonHiddenJobs)
                    {
                        if (job.EncounterId != 0) encounterIdsNeeded.Add(job.EncounterId);
                        if (job.SecondaryEncounterId.HasValue && job.SecondaryEncounterId.Value != 0)
                            encounterIdsNeeded.Add(job.SecondaryEncounterId.Value);
                    }

                    foreach (var encId in encounterIdsNeeded)
                    {
                        if (Seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
                        {
                            hitRateLimitCooldown = true;
                            break;
                        }

                        var cids = nonHiddenJobs
                            .Where(j => j.EncounterId == encId || j.SecondaryEncounterId == encId)
                            .Select(j => j.ContentId)
                            .Distinct()
                            .ToList();

                        // Collect report codes per character (limit)
                        var codesByCid = new Dictionary<ulong, List<string>>();
                        var allCodes = new HashSet<string>();
                        foreach (var cid in cids)
                        {
                            if (!chosenDataByCid.TryGetValue(cid, out var d) || d == null)
                                continue;

                            var codes = d.RecentReportCodes
                                .Take(reportsToCheckForProgress)
                                .Where(code => !string.IsNullOrWhiteSpace(code))
                                .ToList();

                            codesByCid[cid] = codes;
                            foreach (var code in codes)
                                allCodes.Add(code);
                        }

                        if (allCodes.Count == 0)
                            continue;

                        var bestBossByReport = await Seams.ApiClient.FetchBestBossPercentByReportAsync(
                            allCodes.ToList(),
                            (int)encId,
                            difficultyId,
                            Cts.Token
                        );

                        if (Seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
                        {
                            hitRateLimitCooldown = true;
                            break;
                        }

                        foreach (var (cid, codes) in codesByCid)
                        {
                            double? best = null;
                            foreach (var code in codes)
                            {
                                if (!bestBossByReport.TryGetValue(code, out var v))
                                    continue;
                                best = best.HasValue ? Math.Min(best.Value, v) : v;
                            }

                            if (best.HasValue && resultsByContentId.TryGetValue(cid, out var pr))
                            {
                                pr.BossPercentages[(int)encId] = best.Value;
                            }
                        }
                    }

                    if (hitRateLimitCooldown)
                    {
                        break;
                    }

                    results.AddRange(resultsByContentId.Values);
                      
                    // Respect Rate Limit (done in Client mostly but we can pause here too)
                    await Task.Delay(1000, Cts.Token);
                }

                    var shouldAbandonRemainingLeases = hitRateLimitCooldown;

                    // 3. Submit Results (retry-safe)
                    var submitBatch = SubmitBuffer.BuildSubmitBatch(results);
                    if (submitBatch.Count > 0)
                    {
                        if (!leaseSession.TryBuildEndpointUrl("/contribute/fflogs/results", out var submitUrl))
                        {
                            SubmitBuffer.RequeueSubmitBatch(submitBatch);
                            consecutiveFailures = 0;
                            await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                            continue;
                        }

                        if (leaseSession.ShouldDeferProtectedEndpointRequest(
                            ProtectedEndpointCapabilityKind.FflogsResults))
                        {
                            SubmitBuffer.RequeueSubmitBatch(submitBatch);
                            consecutiveFailures = 0;
                            await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                            continue;
                        }

                        var jsonContent = JsonConvert.SerializeObject(submitBatch);
                        try
                        {
                            var submitCapability = leaseSession.TryGetProtectedEndpointCapability(
                                ProtectedEndpointCapabilityKind.FflogsResults,
                                out var cachedCapability)
                                ? cachedCapability
                                : null;
                            using var submitRequest = IngestRequestFactory.CreatePostJsonRequest(
                                Configuration,
                                submitUrl,
                                "/contribute/fflogs/results",
                                jsonContent,
                                submitCapability
                            );
                            var submitResp = await Seams.IngestHttpSender.SendAsync(submitRequest, Cts.Token);
                            var submitRespBody = await submitResp.Content.ReadAsStringAsync(Cts.Token);

                            if (submitResp.IsSuccessStatusCode)
                            {
                                if (TryParseResultsSubmitResponse(submitRespBody, out var parsed))
                                {
                                    InfoLog(
                                        $"Uploaded parse results: updated={parsed.Updated}, accepted={parsed.Accepted}/{parsed.Submitted}, rejected={parsed.Rejected}, status={parsed.Status}.");
                                }
                                else
                                {
                                    InfoLog($"Uploaded {submitBatch.Count} parse results.");
                                }
                            }
                            else
                            {
                                SubmitBuffer.RequeueSubmitBatch(submitBatch);
                                var isAuthFailure = submitResp.StatusCode == System.Net.HttpStatusCode.Forbidden
                                    || submitResp.StatusCode == System.Net.HttpStatusCode.Unauthorized;
                                if (isAuthFailure)
                                {
                                    leaseSession.MarkProtectedEndpointCapabilitiesRequired();
                                    leaseSession.InvalidateProtectedEndpointCapability(
                                        ProtectedEndpointCapabilityKind.FflogsResults);
                                }
                                else
                                {
                                    hadTransientFailure = true;
                                }
                                if ((int)submitResp.StatusCode == 429)
                                {
                                    var retryAfter = IngestRequestFactory.ReadRetryAfterSeconds(submitResp);
                                    if (retryAfter.HasValue)
                                    {
                                        WarningLog($"FFLogsCollector: results endpoint rate limited, retry_after={retryAfter.Value}s");
                                    }
                                }
                                ErrorLog($"Failed to upload results: {submitResp.StatusCode} (requeued {submitBatch.Count}) body={submitRespBody}");
                            }
                        }
                        catch (Exception ex)
                        {
                            hadTransientFailure = true;
                            SubmitBuffer.RequeueSubmitBatch(submitBatch);
                            ErrorLog($"Failed to upload results (exception): {ex.Message} (requeued {submitBatch.Count})");
                        }
                    }

                    if (shouldAbandonRemainingLeases)
                    {
                        await LeaseAbandoner.TryAbandonUnprocessedLeasesAsync(
                            Configuration,
                            leaseSession.UploadUrl,
                            jobs,
                            results,
                            "fflogs_rate_limit_cooldown",
                            Cts.Token,
                            WarningLog,
                            DebugLog
                        );
                    }

                    if (hitRateLimitCooldown)
                    {
                        consecutiveFailures = 0;
                        WorkerPolicy.LogCooldownSkipIfNeeded(cooldownRemaining);
                        await WorkerPolicy.DelayAsync(WorkerPolicy.WorkerBaseDelayMs, Cts.Token);
                        continue;
                    }

                    if (hadTransientFailure)
                    {
                        consecutiveFailures = await WorkerPolicy.DelayWithBackoffAsync(consecutiveFailures, Cts.Token);
                    }
                    else
                    {
                        consecutiveFailures = 0;
                        await WorkerPolicy.DelayAsync(WorkerPolicy.ComputeIdleDelayMs(), Cts.Token);
                    }

            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                ErrorLog($"FFLogsCollector Loop Error: {ex.Message}");
                consecutiveFailures = await WorkerPolicy.DelayWithBackoffAsync(consecutiveFailures, Cts.Token);
            }
        }
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseJob
{
    public ulong ContentId { get; set; }
    public string Name { get; set; } = "";
    public string Server { get; set; } = "";
    public string Region { get; set; } = "";
    public List<ParseJobCandidateServer> CandidateServers { get; set; } = new();
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
    public uint EncounterId { get; set; }
    public uint? SecondaryEncounterId { get; set; }
    public string LeaseToken { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseJobCandidateServer
{
    public string Server { get; set; } = "";
    public string Region { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseResult
{
    public ulong ContentId { get; set; }
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
    public Dictionary<int, double> Encounters { get; set; } = new();
    public Dictionary<int, double> BossPercentages { get; set; } = new();
    public Dictionary<int, int> ClearCounts { get; set; } = new();
    public bool IsHidden { get; set; }
    public bool IsEstimated { get; set; }
    public string MatchedServer { get; set; } = "";
    public string LeaseToken { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class AbandonFflogsLease
{
    public ulong ContentId { get; set; }
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
    public string LeaseToken { get; set; } = "";
    public string Reason { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ContributeFflogsResultsResponse
{
    public string Status { get; set; } = "";
    public int Submitted { get; set; }
    public int Accepted { get; set; }
    public int Updated { get; set; }
    public int Rejected { get; set; }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ContributeFflogsLeaseAbandonResponse
{
    public string Status { get; set; } = "";
    public int Submitted { get; set; }
    public int Released { get; set; }
    public int Rejected { get; set; }
}
