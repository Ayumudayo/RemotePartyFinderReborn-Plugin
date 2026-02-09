using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
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

    public async Task<JObject?> QueryAsync(string query)
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

            var response = await _httpClient.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                Plugin.Log.Error($"FFLogs API Query failed: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            return JObject.Parse(content);
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
}
