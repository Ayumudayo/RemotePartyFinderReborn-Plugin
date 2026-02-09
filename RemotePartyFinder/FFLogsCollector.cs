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

    private async Task WorkerLoop()
    {
        while (!Cts.Token.IsCancellationRequested)
        {
            try
            {
                if (string.IsNullOrEmpty(Plugin.Configuration.FFLogsClientId) || 
                    string.IsNullOrEmpty(Plugin.Configuration.FFLogsClientSecret))
                {
                    await Task.Delay(5000, Cts.Token);
                    continue;
                }

                // Get primary upload URL
                var uploadUrl = Plugin.Configuration.UploadUrls.FirstOrDefault(u => u.IsEnabled);
                if (uploadUrl == null)
                {
                    await Task.Delay(5000, Cts.Token);
                    continue;
                }

                // Circuit Breaker check
                if (uploadUrl.FailureCount >= Plugin.Configuration.CircuitBreakerFailureThreshold)
                {
                    if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < Plugin.Configuration.CircuitBreakerBreakDurationMinutes)
                    {
                        await Task.Delay(5000, Cts.Token);
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
                List<ParseJob>? jobs = null;
                
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
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                        }
                    }
                }
                catch (Exception ex)
                {
                    uploadUrl.FailureCount++;
                    uploadUrl.LastFailureTime = DateTime.UtcNow;
                    Plugin.Log.Debug($"Error requesting work: {ex.Message}");
                }

                if (jobs == null || jobs.Count == 0)
                {
                    // No work, sleep
                    await Task.Delay(10000, Cts.Token);
                    continue;
                }

                Plugin.Log.Debug($"Received {jobs.Count} players to fetch from FFLogs.");

                // 2. Fetch from FFLogs
                // Group by Zone
                var results = new List<ParseResult>();
                var jobsByZone = jobs.GroupBy(j => j.ZoneId);

                foreach (var group in jobsByZone)
                {
                    var zoneId = group.Key;
                    var batch = group.Select(j => (j.ContentId, j.Name, j.Server, j.Region)).ToList();
                    
                    // We assume Difficulty/Partition are consistent or we take first?
                    // Server should probably group by full criteria, but for now assuming Zone is enough
                    // or we check the first one.
                    var first = group.First();
                    
                    var fetched = await FFLogsClient.FetchCharacterParsesBatchAsync(
                        batch, 
                        (int)zoneId, 
                        first.DifficultyId == 0 ? null : first.DifficultyId, 
                        first.Partition == 0 ? null : first.Partition
                    );

                    foreach (var job in group)
                    {
                        if (fetched.TryGetValue(job.ContentId, out var encounters))
                        {
                             var encMap = encounters.ToDictionary(e => e.EncounterId, e => e.Percentile);
                             results.Add(new ParseResult
                             {
                                 ContentId = job.ContentId,
                                 ZoneId = job.ZoneId,
                                 Encounters = encMap
                             });
                        }
                    }
                    
                    // Respect Rate Limit (done in Client mostly but we can pause here too)
                    await Task.Delay(1000, Cts.Token);
                }

                // 3. Submit Results
                if (results.Count > 0)
                {
                    var submitUrl = $"{baseUrl}/contribute/fflogs/results";
                    var jsonContent = JsonConvert.SerializeObject(results);
                     var submitResp = await HttpClient.PostAsync(submitUrl, new StringContent(jsonContent) {
                        Headers = { ContentType = MediaTypeHeaderValue.Parse("application/json") }
                    }, Cts.Token);

                    if (submitResp.IsSuccessStatusCode)
                    {
                         Plugin.Log.Info($"Uploaded {results.Count} parse results.");
                    }
                    else
                    {
                         Plugin.Log.Error($"Failed to upload results: {submitResp.StatusCode}");
                    }
                }

            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Plugin.Log.Error($"FFLogsCollector Loop Error: {ex.Message}");
                await Task.Delay(5000, Cts.Token);
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
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseResult
{
    public ulong ContentId { get; set; }
    public uint ZoneId { get; set; }
    public Dictionary<int, double> Encounters { get; set; } = new();
}
