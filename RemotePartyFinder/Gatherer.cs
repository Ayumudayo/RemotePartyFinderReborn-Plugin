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

    private ConcurrentDictionary<int, ConcurrentQueue<IPartyFinderListing>> Batches { get; } = new();
    private Stopwatch UploadTimer { get; } = new();
    private HttpClient Client { get; } = new();
    private volatile bool _isUploading;

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
        var queue = this.Batches.GetOrAdd(args.BatchNumber, _ => new ConcurrentQueue<IPartyFinderListing>());
        queue.Enqueue(listing);
    }

    private void OnUpdate(IFramework framework1) {
        if (this.UploadTimer.Elapsed < TimeSpan.FromSeconds(3)) {
            return;
        }

        if (this._isUploading) {
            return;
        }

        this.UploadTimer.Restart();

        this._isUploading = true;
        this.UploadPendingBatchesAsync();
    }

    private void UploadPendingBatchesAsync() {
        Task.Run(async () => {
            try {
                var uploadable = new List<UploadableListing>();

                foreach (var (batch, _) in this.Batches.ToList()) {
                    if (!this.Batches.TryRemove(batch, out var queue) || queue == null) {
                        continue;
                    }

                    while (queue.TryDequeue(out var listing)) {
                        uploadable.Add(new UploadableListing(listing));
                    }
                }

                if (uploadable.Count == 0) {
                    return;
                }

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

                        Plugin.Log.Debug($"Gatherer: {targetUrl}: {resp.StatusCode} ({uploadable.Count} listings)");
                    } catch (Exception ex) {
                        uploadUrl.FailureCount++;
                        uploadUrl.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"Gatherer upload error to {targetUrl}: {ex.Message}");
                    }
                }
            } finally {
                this._isUploading = false;
            }
        });
    }
}
