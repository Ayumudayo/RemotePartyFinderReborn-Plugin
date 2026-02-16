using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Dalamud.Plugin.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

public class FFLogsCollector : IDisposable
{
    private Plugin Plugin { get; }
    private FFLogsClient FFLogsClient { get; }
    private HttpClient HttpClient { get; } = new();
    private Dictionary<string, ParseResult> PendingSubmitResults { get; } = new();
    private bool IsRunning { get; set; }
    private System.Threading.CancellationTokenSource Cts { get; set; } = new();

    public FFLogsCollector(Plugin plugin)
    {
        Plugin = plugin;
        FFLogsClient = new FFLogsClient(plugin.Configuration);
        Plugin.Framework.Update += OnUpdate;
        StartWorker();
    }

    public void Dispose()
    {
        Plugin.Framework.Update -= OnUpdate;
        Cts.Cancel();
        FFLogsClient.Dispose();
        HttpClient.Dispose();
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
            if (hit.EncounterId == 0) return;
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

    private static string ParseResultKey(ParseResult result)
        => $"{result.ContentId}:{result.ZoneId}:{result.DifficultyId}:{result.Partition}";

    private List<ParseResult> BuildSubmitBatch(List<ParseResult> freshResults)
    {
        foreach (var result in freshResults)
        {
            this.PendingSubmitResults[ParseResultKey(result)] = result;
        }

        if (this.PendingSubmitResults.Count == 0)
        {
            return new List<ParseResult>();
        }

        var batch = this.PendingSubmitResults.Values.ToList();
        this.PendingSubmitResults.Clear();
        return batch;
    }

    private void RequeueSubmitBatch(IEnumerable<ParseResult> failedBatch)
    {
        foreach (var result in failedBatch)
        {
            this.PendingSubmitResults[ParseResultKey(result)] = result;
        }
    }

    private int WorkerBaseDelayMs
        => Math.Clamp(Plugin.Configuration.FFLogsWorkerBaseDelayMs, 1000, 120000);

    private int WorkerIdleDelayMs
        => Math.Clamp(Plugin.Configuration.FFLogsWorkerIdleDelayMs, 1000, 300000);

    private int WorkerJitterMs
        => Math.Clamp(Plugin.Configuration.FFLogsWorkerJitterMs, 0, 30000);

    private int WorkerMaxBackoffDelayMs
        => Math.Clamp(
            Plugin.Configuration.FFLogsWorkerMaxBackoffDelayMs,
            WorkerBaseDelayMs,
            600000);

    private static int JitteredDelayMs(int baseDelayMs, int jitterMs = 1000)
    {
        if (baseDelayMs < 0)
            baseDelayMs = 0;
        if (jitterMs < 0)
            jitterMs = 0;

        return baseDelayMs + Random.Shared.Next(0, jitterMs + 1);
    }

    private Task DelayWithJitterAsync(int baseDelayMs, int jitterMs = 1000)
        => Task.Delay(JitteredDelayMs(baseDelayMs, jitterMs), Cts.Token);

    private async Task<int> DelayWithBackoffAsync(int consecutiveFailures)
    {
        var nextFailures = Math.Min(consecutiveFailures + 1, 16);
        var exponent = Math.Min(nextFailures - 1, 4);
        var backoffDelay = Math.Min(WorkerMaxBackoffDelayMs, WorkerBaseDelayMs * (1 << exponent));
        await DelayWithJitterAsync(backoffDelay, WorkerJitterMs);
        return nextFailures;
    }
 
    private async Task WorkerLoop()
    {
        var consecutiveFailures = 0;
        while (!Cts.Token.IsCancellationRequested)
        {
            try
            {
                var hadTransientFailure = false;

                if (!Plugin.Configuration.EnableFFLogsWorker)
                {
                    consecutiveFailures = 0;
                    await DelayWithJitterAsync(WorkerBaseDelayMs, WorkerJitterMs);
                    continue;
                }

                if (string.IsNullOrEmpty(Plugin.Configuration.FFLogsClientId) || 
                    string.IsNullOrEmpty(Plugin.Configuration.FFLogsClientSecret))
                {
                    consecutiveFailures = 0;
                    await DelayWithJitterAsync(WorkerBaseDelayMs, WorkerJitterMs);
                    continue;
                }

                // Get primary upload URL
                var uploadUrl = Plugin.Configuration.UploadUrls.FirstOrDefault(u => u.IsEnabled);
                if (uploadUrl == null)
                {
                    consecutiveFailures = 0;
                    await DelayWithJitterAsync(WorkerBaseDelayMs, WorkerJitterMs);
                    continue;
                }

                // Circuit Breaker check
                if (uploadUrl.FailureCount >= Plugin.Configuration.CircuitBreakerFailureThreshold)
                {
                    if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < Plugin.Configuration.CircuitBreakerBreakDurationMinutes)
                    {
                        consecutiveFailures = 0;
                        await DelayWithJitterAsync(WorkerBaseDelayMs, WorkerJitterMs);
                        continue;
                    }
                }

                var baseUrl = uploadUrl.Url.TrimEnd('/');
                // Fix base URL if needed (same logic as others)
                if (baseUrl.EndsWith("/contribute/multiple"))
                    baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
                else if (baseUrl.EndsWith("/contribute"))
                    baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);

                // 1. Request Work
                var workUrl = $"{baseUrl}/contribute/fflogs/jobs";
                List<ParseJob> jobs = null;
                
                try 
                {
                    var response = await HttpClient.GetAsync(workUrl, Cts.Token);
                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync(Cts.Token);
                        jobs = JsonConvert.DeserializeObject<List<ParseJob>>(json);
                        uploadUrl.FailureCount = 0;
                    }
                    else
                    {
                        // 404 or other error means server might not support this or is down
                         // Log only if not 404 to avoid spam on old servers
                        if (response.StatusCode != System.Net.HttpStatusCode.NotFound)
                        {
                            hadTransientFailure = true;
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                        }
                    }
                }
                catch (Exception ex)
                {
                    hadTransientFailure = true;
                    uploadUrl.FailureCount++;
                    uploadUrl.LastFailureTime = DateTime.UtcNow;
                    Plugin.Log.Debug($"Error requesting work: {ex.Message}");
                }

                if (jobs == null || jobs.Count == 0)
                {
                    if (hadTransientFailure)
                    {
                        consecutiveFailures = await DelayWithBackoffAsync(consecutiveFailures);
                    }
                    else
                    {
                        consecutiveFailures = 0;
                        await DelayWithJitterAsync(WorkerIdleDelayMs, WorkerJitterMs);
                    }
                    continue;
                }

                Plugin.Log.Debug($"Received {jobs.Count} players to fetch from FFLogs.");

                // 2. Fetch from FFLogs
                // Group by Zone (+ criteria)
                var results = new List<ParseResult>();
                var jobsByZone = jobs.GroupBy(j => new { j.ZoneId, j.DifficultyId, j.Partition });

                const int recentReportsLimit = 10;
                const int reportsToCheckForProgress = 5;

                foreach (var group in jobsByZone)
                {
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

                    var fetchedByKey = await FFLogsClient.FetchCharacterCandidateDataBatchAsync(
                        candidateQueries,
                        (int)zoneId,
                        difficultyId,
                        partition,
                        recentReportsLimit,
                        Cts.Token
                    );

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
                        };

                        if (!pr.IsHidden)
                        {
                            pr.Encounters = bestData.Parses.ToDictionary(e => e.EncounterId, e => e.Percentile);
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

                        var bestBossByReport = await FFLogsClient.FetchBestBossPercentByReportAsync(
                            allCodes.ToList(),
                            (int)encId,
                            difficultyId,
                            Cts.Token
                        );

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

                    results.AddRange(resultsByContentId.Values);
                     
                    // Respect Rate Limit (done in Client mostly but we can pause here too)
                    await Task.Delay(1000, Cts.Token);
                }

                    // 3. Submit Results (retry-safe)
                    var submitBatch = BuildSubmitBatch(results);
                    if (submitBatch.Count > 0)
                    {
                        var submitUrl = $"{baseUrl}/contribute/fflogs/results";
                        var jsonContent = JsonConvert.SerializeObject(submitBatch);
                        try
                        {
                            var submitResp = await HttpClient.PostAsync(submitUrl, new StringContent(jsonContent)
                            {
                                Headers = { ContentType = MediaTypeHeaderValue.Parse("application/json") }
                            }, Cts.Token);

                            if (submitResp.IsSuccessStatusCode)
                            {
                                Plugin.Log.Info($"Uploaded {submitBatch.Count} parse results.");
                            }
                            else
                            {
                                hadTransientFailure = true;
                                RequeueSubmitBatch(submitBatch);
                                Plugin.Log.Error($"Failed to upload results: {submitResp.StatusCode} (requeued {submitBatch.Count})");
                            }
                        }
                        catch (Exception ex)
                        {
                            hadTransientFailure = true;
                            RequeueSubmitBatch(submitBatch);
                            Plugin.Log.Error($"Failed to upload results (exception): {ex.Message} (requeued {submitBatch.Count})");
                        }
                    }

                    if (hadTransientFailure)
                    {
                        consecutiveFailures = await DelayWithBackoffAsync(consecutiveFailures);
                    }
                    else
                    {
                        consecutiveFailures = 0;
                        await DelayWithJitterAsync(WorkerIdleDelayMs, WorkerJitterMs);
                    }

            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Plugin.Log.Error($"FFLogsCollector Loop Error: {ex.Message}");
                consecutiveFailures = await DelayWithBackoffAsync(consecutiveFailures);
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
    public bool IsHidden { get; set; }
    public bool IsEstimated { get; set; }
    public string MatchedServer { get; set; } = "";
}
