using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Dalamud.Game.Gui.PartyFinder.Types;
using Dalamud.Plugin.Services;
using Newtonsoft.Json;

namespace RemotePartyFinder;

internal class Gatherer : IDisposable {
    private Plugin Plugin { get; }

    private ConcurrentDictionary<int, List<IPartyFinderListing>> Batches { get; } = new();
    private Stopwatch UploadTimer { get; } = new();
    private HttpClient Client { get; } = new();

    internal Gatherer(Plugin plugin) {
        this.Plugin = plugin;

        this.UploadTimer.Start();

        this.Plugin.PartyFinderGui.ReceiveListing += this.OnListing;
        this.Plugin.Framework.Update += this.OnUpdate;
    }

    public void Dispose() {
        this.Plugin.Framework.Update -= this.OnUpdate;
        this.Plugin.PartyFinderGui.ReceiveListing -= this.OnListing;
    }

    private void OnListing(IPartyFinderListing listing, IPartyFinderListingEventArgs args) {
        if (!this.Batches.ContainsKey(args.BatchNumber)) {
            this.Batches[args.BatchNumber] = [];
        }

        this.Batches[args.BatchNumber].Add(listing);
    }

    private void OnUpdate(IFramework framework1) {
        if (this.UploadTimer.Elapsed < TimeSpan.FromSeconds(3)) {
            return;
        }

        this.UploadTimer.Restart();

        foreach (var (batch, listings) in this.Batches.ToList()) {
            this.Batches.Remove(batch, out _);
            Task.Run(async () => {
                var uploadable = listings
                    .Select(listing => new UploadableListing(listing))
                    .ToList();
                var json = JsonConvert.SerializeObject(uploadable);

                foreach (var uploadUrl in Plugin.Configuration.UploadUrls.Where(uploadUrl => uploadUrl.IsEnabled))
                {
                    // Circuit Breaker: 설정된 횟수 이상 실패 시 설정된 시간(분)만큼 중단
                    if (uploadUrl.FailureCount >= Plugin.Configuration.CircuitBreakerFailureThreshold) {
                        if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < Plugin.Configuration.CircuitBreakerBreakDurationMinutes) {
                            continue;
                        }
                        // 설정된 시간이 지났으면 재시도 허용 (Half-Open)
                    }

                    var baseUrl = uploadUrl.Url.TrimEnd('/');

                    if (baseUrl.EndsWith("/contribute/multiple")) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
                    } else if (baseUrl.EndsWith("/contribute")) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
                    }

                    var targetUrl = baseUrl + "/contribute/multiple";

                    try {
                        var resp = await this.Client.PostAsync(targetUrl, new StringContent(json) {
                            Headers = { ContentType = MediaTypeHeaderValue.Parse("application/json") },
                        });
                        
                        // 성공 여부 확인
                        if (resp.IsSuccessStatusCode) {
                            uploadUrl.FailureCount = 0;
                        } else {
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                        }
                        
                        var output = await resp.Content.ReadAsStringAsync();
                        Plugin.Log.Info($"{targetUrl}: {resp.StatusCode}\n{output}");
                    } catch (Exception ex) {
                        uploadUrl.FailureCount++;
                        uploadUrl.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"Gatherer upload error to {targetUrl}: {ex.Message}");
                    }
                }
            });
        }
    }
}
