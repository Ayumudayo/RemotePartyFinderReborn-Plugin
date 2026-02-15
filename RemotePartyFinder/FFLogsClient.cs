using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Dalamud.Logging;

namespace RemotePartyFinder;

public class FFLogsClient : IDisposable
{
    private readonly Configuration _configuration;
    private readonly HttpClient _httpClient;
    private string? _accessToken;
    private DateTime _tokenExpiration;

    private DateTime _lastGraphQlErrorLog = DateTime.MinValue;
    private string _lastGraphQlErrorMessage = string.Empty;

    private const string TokenUrl = "https://www.fflogs.com/oauth/token";
    private const string GraphQlUrl = "https://www.fflogs.com/api/v2/client";

    public FFLogsClient(Configuration configuration)
    {
        _configuration = configuration;
        _httpClient = new HttpClient();
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    private async Task<bool> EnsureTokenAsync()
    {
        if (!string.IsNullOrEmpty(_accessToken) && DateTime.UtcNow < _tokenExpiration)
        {
            return true;
        }

        if (string.IsNullOrEmpty(_configuration.FFLogsClientId) || string.IsNullOrEmpty(_configuration.FFLogsClientSecret))
        {
            return false;
        }

        try
        {
            var request = new HttpRequestMessage(HttpMethod.Post, TokenUrl);
            var form = new Dictionary<string, string>
            {
                { "grant_type", "client_credentials" },
                { "client_id", _configuration.FFLogsClientId },
                { "client_secret", _configuration.FFLogsClientSecret }
            };
            request.Content = new FormUrlEncodedContent(form);

            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync();
            var json = JObject.Parse(content);

            _accessToken = json["access_token"]?.ToString();
            var expiresIn = json["expires_in"]?.ToObject<int>() ?? 0;
            _tokenExpiration = DateTime.UtcNow.AddSeconds(expiresIn - 60); // Buffer 60s

            return !string.IsNullOrEmpty(_accessToken);
        }
        catch (Exception ex)
        {
            Plugin.Log.Error(ex, "Failed to authenticate with FFLogs.");
            return false;
        }
    }

    public Task<JObject?> QueryAsync(string query)
        => QueryAsync(query, CancellationToken.None);

    public async Task<JObject?> QueryAsync(string query, CancellationToken cancellationToken)
    {
        if (!await EnsureTokenAsync())
        {
            return null;
        }

        try
        {
            var request = new HttpRequestMessage(HttpMethod.Post, GraphQlUrl);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
             
            var payload = new { query };
            request.Content = new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json");

            var response = await _httpClient.SendAsync(request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                Plugin.Log.Error($"FFLogs API Query failed: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            var json = JObject.Parse(content);

            if (json["errors"] is JArray errors && errors.Count > 0)
            {
                var msg = errors[0]?["message"]?.ToString();
                if (!string.IsNullOrWhiteSpace(msg))
                {
                    var now = DateTime.UtcNow;
                    if (!string.Equals(msg, _lastGraphQlErrorMessage, StringComparison.Ordinal)
                        || (now - _lastGraphQlErrorLog) > TimeSpan.FromSeconds(60))
                    {
                        _lastGraphQlErrorMessage = msg;
                        _lastGraphQlErrorLog = now;
                        Plugin.Log.Error($"FFLogs GraphQL error: {msg}");
                    }
                }
            }

            return json;
        }
        catch (Exception ex)
        {
            Plugin.Log.Error(ex, "Failed to query FFLogs API.");
            return null;
        }
    }

    public async Task<Dictionary<ulong, List<(int EncounterId, double Percentile)>>> FetchCharacterParsesBatchAsync(
        List<(ulong ContentId, string Name, string Server, string Region)> characters,
        int zoneId,
        int? difficultyId = null,
        int? partition = null)
    {
        if (characters.Count == 0) return new();

        var sb = new StringBuilder();
        sb.Append("query { characterData {");

        for (int i = 0; i < characters.Count; i++)
        {
            var charInfo = characters[i];
            // Alias: c{index}
            sb.Append($" c{i}: character(name: \"{charInfo.Name}\", serverSlug: \"{charInfo.Server}\", serverRegion: \"{charInfo.Region}\") {{");
            
            // Build zoneRankings arguments
            var args = new List<string> { $"zoneID: {zoneId}" };
            if (difficultyId.HasValue) args.Add($"difficulty: {difficultyId.Value}");
            if (partition.HasValue) args.Add($"partition: {partition.Value}");
            args.Add("metric: rdps");
            args.Add("timeframe: Historical");

            sb.Append($" zoneRankings({string.Join(", ", args)})");
            sb.Append(" }");
        }

        sb.Append(" }}");

        var result = await QueryAsync(sb.ToString());
        var output = new Dictionary<ulong, List<(int, double)>>();

        if (result?["data"]?["characterData"] is not JObject data)
        {
            return output;
        }

        for (int i = 0; i < characters.Count; i++)
        {
            var charInfo = characters[i];
            var alias = $"c{i}";
            var characterData = data[alias];
             
            var encounterParses = new List<(int, double)>();

            if (characterData is JObject && characterData["zoneRankings"]?["rankings"] is JArray rankings)
            {
                foreach (var rank in rankings)
                {
                    if (rank is not JObject) continue;

                    var encounterId = rank["encounter"]?["id"]?.ToObject<int>();
                    var percentile = rank["rankPercent"]?.ToObject<double?>();

                    if (encounterId.HasValue && percentile.HasValue)
                    {
                        encounterParses.Add((encounterId.Value, percentile.Value));
                    }
                }
            }
            
            output[charInfo.ContentId] = encounterParses;
        }

        return output;
    }

    public sealed class CharacterFetchedData
    {
        public bool Hidden { get; init; }
        public List<(int EncounterId, double Percentile)> Parses { get; init; } = new();
        public List<string> RecentReportCodes { get; init; } = new();
    }

    public sealed class CandidateCharacterQuery
    {
        public string Key { get; set; } = "";
        public string Name { get; set; } = "";
        public string Server { get; set; } = "";
        public string Region { get; set; } = "";
    }

    public async Task<Dictionary<ulong, CharacterFetchedData>> FetchCharacterDataBatchAsync(
        List<(ulong ContentId, string Name, string Server, string Region)> characters,
        int zoneId,
        int? difficultyId,
        int? partition,
        int recentReportsLimit,
        CancellationToken cancellationToken)
    {
        if (characters.Count == 0) return new();

        var sb = new StringBuilder();
        sb.Append("query { characterData {");

        for (var i = 0; i < characters.Count; i++)
        {
            var charInfo = characters[i];
            var name = EscapeGraphQlString(charInfo.Name);
            var server = EscapeGraphQlString(charInfo.Server);
            var region = EscapeGraphQlString(charInfo.Region);

            sb.Append($" c{i}: character(name: \"{name}\", serverSlug: \"{server}\", serverRegion: \"{region}\") {{");
            sb.Append(" hidden ");

            // zoneRankings (parse)
            var args = new List<string> { $"zoneID: {zoneId}" };
            if (difficultyId.HasValue) args.Add($"difficulty: {difficultyId.Value}");
            if (partition.HasValue) args.Add($"partition: {partition.Value}");
            args.Add("metric: rdps");
            args.Add("timeframe: Historical");

            sb.Append($" zoneRankings({string.Join(", ", args)}) ");

            // recent reports (for progress lookup)
            if (recentReportsLimit > 0)
            {
                sb.Append($" recentReports(limit: {recentReportsLimit}) {{ data {{ code }} }} ");
            }

            sb.Append(" }");
        }

        sb.Append(" } }");

        var result = await QueryAsync(sb.ToString(), cancellationToken);
        var output = new Dictionary<ulong, CharacterFetchedData>();

        if (result?[
                "data"]?[
                "characterData"] is not JObject data)
        {
            return output;
        }

        for (var i = 0; i < characters.Count; i++)
        {
            var charInfo = characters[i];
            var alias = $"c{i}";
            if (data[alias] is not JObject character)
            {
                // Character not found on that server/region.
                // Return empty data so the server can negative-cache this lookup.
                output[charInfo.ContentId] = new CharacterFetchedData { Hidden = false };
                continue;
            }

            var hidden = character["hidden"]?.ToObject<bool?>() ?? false;
            var parses = new List<(int EncounterId, double Percentile)>();

            if (character["zoneRankings"]?["rankings"] is JArray rankings)
            {
                foreach (var rank in rankings)
                {
                    if (rank is not JObject) continue;
                    var encounterId = rank["encounter"]?["id"]?.ToObject<int?>();
                    var percentile = rank["rankPercent"]?.ToObject<double?>();
                    if (encounterId.HasValue && percentile.HasValue)
                    {
                        parses.Add((encounterId.Value, percentile.Value));
                    }
                }
            }

            var reportCodes = new List<string>();
            if (recentReportsLimit > 0 && character["recentReports"]?["data"] is JArray reports)
            {
                foreach (var r in reports)
                {
                    var code = r?["code"]?.ToString();
                    if (!string.IsNullOrWhiteSpace(code))
                    {
                        reportCodes.Add(code);
                    }
                }
            }

            output[charInfo.ContentId] = new CharacterFetchedData
            {
                Hidden = hidden,
                Parses = parses,
                RecentReportCodes = reportCodes,
            };
        }

        return output;
    }

    public async Task<Dictionary<string, CharacterFetchedData>> FetchCharacterCandidateDataBatchAsync(
        List<CandidateCharacterQuery> candidates,
        int zoneId,
        int? difficultyId,
        int? partition,
        int recentReportsLimit,
        CancellationToken cancellationToken)
    {
        var output = new Dictionary<string, CharacterFetchedData>(StringComparer.OrdinalIgnoreCase);
        if (candidates.Count == 0) return output;

        const int chunkSize = 40;

        for (var offset = 0; offset < candidates.Count; offset += chunkSize)
        {
            var chunk = candidates.Skip(offset).Take(chunkSize).ToList();

            var sb = new StringBuilder();
            sb.Append("query { characterData {");

            for (var i = 0; i < chunk.Count; i++)
            {
                var q = chunk[i];
                var name = EscapeGraphQlString(q.Name);
                var server = EscapeGraphQlString(q.Server);
                var region = EscapeGraphQlString(q.Region);

                sb.Append($" c{i}: character(name: \"{name}\", serverSlug: \"{server}\", serverRegion: \"{region}\") {{");
                sb.Append(" hidden ");

                var args = new List<string> { $"zoneID: {zoneId}" };
                if (difficultyId.HasValue) args.Add($"difficulty: {difficultyId.Value}");
                if (partition.HasValue) args.Add($"partition: {partition.Value}");
                args.Add("metric: rdps");
                args.Add("timeframe: Historical");
                sb.Append($" zoneRankings({string.Join(", ", args)}) ");

                if (recentReportsLimit > 0)
                {
                    sb.Append($" recentReports(limit: {recentReportsLimit}) {{ data {{ code }} }} ");
                }

                sb.Append(" }");
            }

            sb.Append(" } }");

            var result = await QueryAsync(sb.ToString(), cancellationToken);
            if (result?["data"]?["characterData"] is not JObject data)
            {
                continue;
            }

            for (var i = 0; i < chunk.Count; i++)
            {
                var alias = $"c{i}";
                if (data[alias] is not JObject character)
                {
                    // Character not found on that server/region.
                    output[chunk[i].Key] = new CharacterFetchedData { Hidden = false };
                    continue;
                }

                var hidden = character["hidden"]?.ToObject<bool?>() ?? false;
                var parses = new List<(int EncounterId, double Percentile)>();

                if (character["zoneRankings"]?["rankings"] is JArray rankings)
                {
                    foreach (var rank in rankings)
                    {
                        if (rank is not JObject) continue;
                        var encounterId = rank["encounter"]?["id"]?.ToObject<int?>();
                        var percentile = rank["rankPercent"]?.ToObject<double?>();
                        if (encounterId.HasValue && percentile.HasValue)
                        {
                            parses.Add((encounterId.Value, percentile.Value));
                        }
                    }
                }

                var reportCodes = new List<string>();
                if (recentReportsLimit > 0 && character["recentReports"]?["data"] is JArray reports)
                {
                    foreach (var r in reports)
                    {
                        var code = r?["code"]?.ToString();
                        if (!string.IsNullOrWhiteSpace(code))
                        {
                            reportCodes.Add(code);
                        }
                    }
                }

                output[chunk[i].Key] = new CharacterFetchedData
                {
                    Hidden = hidden,
                    Parses = parses,
                    RecentReportCodes = reportCodes,
                };
            }
        }

        return output;
    }

    public async Task<Dictionary<string, double>> FetchBestBossPercentByReportAsync(
        List<string> reportCodes,
        int encounterId,
        int? difficultyId,
        CancellationToken cancellationToken)
    {
        var output = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        if (reportCodes.Count == 0) return output;

        const int chunkSize = 25;

        for (var offset = 0; offset < reportCodes.Count; offset += chunkSize)
        {
            var chunk = reportCodes.Skip(offset).Take(chunkSize).ToList();

            var sb = new StringBuilder();
            sb.Append("query { reportData {");

            for (var i = 0; i < chunk.Count; i++)
            {
                var code = EscapeGraphQlString(chunk[i]);
                sb.Append($" r{i}: report(code: \"{code}\") {{");

                sb.Append($" fights(encounterID: {encounterId}, killType: Encounters");
                if (difficultyId.HasValue)
                {
                    sb.Append($", difficulty: {difficultyId.Value}");
                }
                sb.Append(") { kill bossPercentage } ");
                sb.Append(" }");
            }

            sb.Append(" } }");

            var result = await QueryAsync(sb.ToString(), cancellationToken);
            if (result?["data"]?["reportData"] is not JObject reportData)
            {
                continue;
            }

            for (var i = 0; i < chunk.Count; i++)
            {
                var code = chunk[i];
                var alias = $"r{i}";
                if (reportData[alias] is not JObject report)
                {
                    continue;
                }

                if (report["fights"] is not JArray fights)
                {
                    continue;
                }

                double? best = null;
                foreach (var f in fights)
                {
                    if (f is not JObject fight) continue;
                    var bp = fight["bossPercentage"]?.ToObject<double?>();
                    if (!bp.HasValue) continue;
                    best = best.HasValue ? Math.Min(best.Value, bp.Value) : bp.Value;
                }

                if (best.HasValue)
                {
                    output[code] = best.Value;
                }
            }
        }

        return output;
    }

    private static string EscapeGraphQlString(string s)
        => s.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
