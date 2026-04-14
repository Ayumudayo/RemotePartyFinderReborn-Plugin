using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RemotePartyFinder;

internal sealed class FFLogsBatchProcessor
{
    private readonly FFLogsCollectorSeams _seams;

    public FFLogsBatchProcessor(FFLogsCollectorSeams seams)
    {
        _seams = seams ?? throw new ArgumentNullException(nameof(seams));
    }

    public async Task<FFLogsBatchProcessResult> ProcessLeaseSessionAsync(
        FFLogsLeaseSession leaseSession,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(leaseSession);

        var results = new List<ParseResult>();
        var jobsByZone = leaseSession.Jobs.GroupBy(j => new { j.ZoneId, j.DifficultyId, j.Partition });

        const int recentReportsLimit = 10;
        const int reportsToCheckForProgress = 5;
        var hitRateLimitCooldown = false;
        var cooldownRemaining = TimeSpan.Zero;

        foreach (var group in jobsByZone)
        {
            if (_seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
            {
                hitRateLimitCooldown = true;
                break;
            }

            var zoneId = group.Key.ZoneId;
            var difficultyId = group.Key.DifficultyId == 0 ? (int?)null : group.Key.DifficultyId;
            var partition = group.Key.Partition == 0 ? (int?)null : group.Key.Partition;

            var uniqueJobs = group
                .GroupBy(j => j.ContentId)
                .Select(g => g.First())
                .ToList();

            var candidatesByCid = new Dictionary<ulong, List<ParseJobCandidateServer>>();
            var candidateQueries = new List<FFLogsClient.CandidateCharacterQuery>();

            foreach (var job in uniqueJobs)
            {
                var candidates = GetCandidates(job);
                if (candidates.Count == 0)
                {
                    continue;
                }

                candidatesByCid[job.ContentId] = candidates;

                for (var i = 0; i < candidates.Count; i++)
                {
                    var candidate = candidates[i];
                    candidateQueries.Add(new FFLogsClient.CandidateCharacterQuery
                    {
                        Key = $"{job.ContentId}:{i}",
                        Name = job.Name,
                        Server = candidate.Server,
                        Region = candidate.Region,
                    });
                }
            }

            var fetchedByKey = await _seams.ApiClient.FetchCharacterCandidateDataBatchAsync(
                candidateQueries,
                (int)zoneId,
                difficultyId,
                partition,
                recentReportsLimit,
                cancellationToken);

            if (_seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
            {
                hitRateLimitCooldown = true;
                break;
            }

            var resultsByContentId = new Dictionary<ulong, ParseResult>();
            var chosenDataByCid = new Dictionary<ulong, FFLogsClient.CharacterFetchedData>();

            foreach (var job in uniqueJobs)
            {
                if (!candidatesByCid.TryGetValue(job.ContentId, out var candidates) || candidates.Count == 0)
                {
                    continue;
                }

                var bestIdx = -1;
                var bestScore = long.MinValue;
                FFLogsClient.CharacterFetchedData bestData = null;

                for (var i = 0; i < candidates.Count; i++)
                {
                    var key = $"{job.ContentId}:{i}";
                    if (!fetchedByKey.TryGetValue(key, out var data))
                    {
                        continue;
                    }

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
                    continue;
                }

                var matched = candidates[bestIdx];
                var parseResult = new ParseResult
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

                if (!parseResult.IsHidden)
                {
                    parseResult.Encounters = bestData.Parses
                        .GroupBy(e => e.EncounterId)
                        .ToDictionary(g => g.Key, g => g.Max(x => x.Percentile));

                    parseResult.ClearCounts = bestData.Parses
                        .Where(e => e.ClearCount.HasValue && e.ClearCount.Value > 0)
                        .GroupBy(e => e.EncounterId)
                        .ToDictionary(g => g.Key, g => g.Max(x => x.ClearCount!.Value));
                }

                resultsByContentId[job.ContentId] = parseResult;
                chosenDataByCid[job.ContentId] = bestData;
            }

            var nonHiddenJobs = uniqueJobs
                .Where(j => resultsByContentId.TryGetValue(j.ContentId, out var result) && !result.IsHidden)
                .ToList();

            var encounterIdsNeeded = new HashSet<uint>();
            foreach (var job in nonHiddenJobs)
            {
                if (job.EncounterId != 0)
                {
                    encounterIdsNeeded.Add(job.EncounterId);
                }

                if (job.SecondaryEncounterId.HasValue && job.SecondaryEncounterId.Value != 0)
                {
                    encounterIdsNeeded.Add(job.SecondaryEncounterId.Value);
                }
            }

            foreach (var encounterId in encounterIdsNeeded)
            {
                if (_seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
                {
                    hitRateLimitCooldown = true;
                    break;
                }

                var cids = nonHiddenJobs
                    .Where(j => j.EncounterId == encounterId || j.SecondaryEncounterId == encounterId)
                    .Select(j => j.ContentId)
                    .Distinct()
                    .ToList();

                var codesByCid = new Dictionary<ulong, List<string>>();
                var allCodes = new HashSet<string>();
                foreach (var contentId in cids)
                {
                    if (!chosenDataByCid.TryGetValue(contentId, out var data) || data == null)
                    {
                        continue;
                    }

                    var codes = data.RecentReportCodes
                        .Take(reportsToCheckForProgress)
                        .Where(code => !string.IsNullOrWhiteSpace(code))
                        .ToList();

                    codesByCid[contentId] = codes;
                    foreach (var code in codes)
                    {
                        allCodes.Add(code);
                    }
                }

                if (allCodes.Count == 0)
                {
                    continue;
                }

                var bestBossByReport = await _seams.ApiClient.FetchBestBossPercentByReportAsync(
                    allCodes.ToList(),
                    (int)encounterId,
                    difficultyId,
                    cancellationToken);

                if (_seams.ApiClient.TryGetRateLimitRemaining(out cooldownRemaining))
                {
                    hitRateLimitCooldown = true;
                    break;
                }

                foreach (var (contentId, codes) in codesByCid)
                {
                    double? best = null;
                    foreach (var code in codes)
                    {
                        if (!bestBossByReport.TryGetValue(code, out var value))
                        {
                            continue;
                        }

                        best = best.HasValue ? Math.Min(best.Value, value) : value;
                    }

                    if (best.HasValue && resultsByContentId.TryGetValue(contentId, out var parseResult))
                    {
                        parseResult.BossPercentages[(int)encounterId] = best.Value;
                    }
                }
            }

            if (hitRateLimitCooldown)
            {
                break;
            }

            results.AddRange(resultsByContentId.Values);
            await Task.Delay(1000, cancellationToken);
        }

        return new FFLogsBatchProcessResult(
            ProcessedResults: results,
            HadTransientFailure: false,
            HitRateLimitCooldown: hitRateLimitCooldown,
            CooldownRemaining: cooldownRemaining,
            ShouldAbandonRemainingLeases: hitRateLimitCooldown);
    }

    private static long ScoreCandidate(
        FFLogsClient.CharacterFetchedData data,
        uint encounterId,
        uint? secondaryEncounterId)
    {
        long score = 0;

        // Slightly prefer visible rankings, but keep parse volume as a separate signal.
        if (!data.Hidden)
        {
            score += 1;
        }

        if (data.Parses.Count > 0)
        {
            score += 1000 + data.Parses.Count;
        }

        void ScoreEncounter(uint? encounter)
        {
            if (!encounter.HasValue || encounter.Value == 0)
            {
                return;
            }

            var hit = data.Parses.FirstOrDefault(p => p.EncounterId == (int)encounter.Value);
            if (hit == null)
            {
                return;
            }

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
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = new List<ParseJobCandidateServer>();
            foreach (var candidate in job.CandidateServers)
            {
                var server = (candidate?.Server ?? string.Empty).Trim();
                var region = (candidate?.Region ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(region))
                {
                    continue;
                }

                var key = server + "|" + region;
                if (!seen.Add(key))
                {
                    continue;
                }

                list.Add(new ParseJobCandidateServer { Server = server, Region = region });
            }

            return list;
        }

        if (!string.IsNullOrWhiteSpace(job.Server) && !string.IsNullOrWhiteSpace(job.Region))
        {
            return
            [
                new ParseJobCandidateServer
                {
                    Server = job.Server.Trim(),
                    Region = job.Region.Trim(),
                },
            ];
        }

        return [];
    }
}
