using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

/// <summary>
/// AgentLookingForGroup.Detailed에서 PF 디테일 정보를 수집합니다.
/// </summary>
internal class PartyDetailCollector : IDisposable {
    private Plugin Plugin { get; }
    private HttpClient Client { get; } = new();
    private System.Diagnostics.Stopwatch ScanTimer { get; } = new();
    private bool wasAddonOpen = false;

    internal PartyDetailCollector(Plugin plugin) {
        this.Plugin = plugin;
        this.ScanTimer.Start();
        this.Plugin.Framework.Update += this.OnUpdate;
    }

    public void Dispose() {
        this.Plugin.Framework.Update -= this.OnUpdate;
    }

    private unsafe void OnUpdate(IFramework framework) {
        // 성능 최적화: 200ms마다 체크
        if (this.ScanTimer.ElapsedMilliseconds < 200) return;
        this.ScanTimer.Restart();

        // UI 창(Addon)이 열려있는지 확인
        nint addonPtr = this.Plugin.GameGui.GetAddonByName("LookingForGroupDetail", 1);
        if (addonPtr == 0) {
            this.wasAddonOpen = false;
            return;
        }

        // 창이 이미 열려있고 이번 세션에서 처리 완료했으면 아무것도 안 함
        if (this.wasAddonOpen) {
            return;
        }

        // 창이 방금 열렸음 - 처리 시작
        this.wasAddonOpen = true;

        var agent = AgentLookingForGroup.Instance();
        if (agent == null) return;

        ref var detailed = ref agent->LastViewedListing;
        if (detailed.ListingId == 0) return;

        var leaderContentId = detailed.LeaderContentId;
        var homeWorld = detailed.HomeWorld;
        var leaderName = agent->LastLeader.ToString();

        // 유효성 검사
        if (leaderContentId == 0 || homeWorld == 0 || homeWorld >= 1000) return;

        var memberContentIds = new List<ulong>();
        for (var i = 0; i < detailed.TotalSlots && i < 48; i++) {
            memberContentIds.Add(detailed.MemberContentIds[i]);
        }

        var uploadData = new UploadablePartyDetail {
            ListingId = detailed.ListingId,
            LeaderContentId = leaderContentId,
            LeaderName = leaderName,
            HomeWorld = homeWorld,
            MemberContentIds = memberContentIds,
        };

        Plugin.Log.Debug(
            $"PartyDetailCollector: Uploading detail listing={uploadData.ListingId} leader={uploadData.LeaderContentId} world={uploadData.HomeWorld} members={uploadData.MemberContentIds.Count}");

        UploadDetailAsync(uploadData);
    }

    private void UploadDetailAsync(UploadablePartyDetail detail) {
        Task.Run(async () => {
            try {
                var json = JsonConvert.SerializeObject(detail);
                foreach (var uploadUrl in this.Plugin.Configuration.UploadUrls.Where(u => u.IsEnabled)) {
                    // Circuit Breaker
                    if (uploadUrl.FailureCount >= this.Plugin.Configuration.CircuitBreakerFailureThreshold) {
                        if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < this.Plugin.Configuration.CircuitBreakerBreakDurationMinutes) {
                            continue;
                        }
                    }

                    var baseUrl = uploadUrl.Url.TrimEnd('/');

                    if (baseUrl.EndsWith("/contribute/multiple")) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
                    } else if (baseUrl.EndsWith("/contribute")) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
                    }

                    var detailUrl = baseUrl + "/contribute/detail";

                    try {
                        var resp = await this.Client.PostAsync(detailUrl, new StringContent(json) {
                            Headers = { ContentType = MediaTypeHeaderValue.Parse("application/json") },
                        });

                        if (resp.IsSuccessStatusCode) {
                            uploadUrl.FailureCount = 0;
                        } else {
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                        }

                        var output = await resp.Content.ReadAsStringAsync();
                        Plugin.Log.Debug($"PartyDetailCollector: {detailUrl}: {resp.StatusCode} {output}");
                    } catch (Exception ex) {
                        uploadUrl.FailureCount++;
                        uploadUrl.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"PartyDetailCollector upload error to {detailUrl}: {ex.Message}");
                    }
                }
            } catch (Exception e) {
                Plugin.Log.Error($"PartyDetailCollector upload error: {e.Message}");
            }
        });
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal class UploadablePartyDetail {
    public uint ListingId { get; set; }
    public ulong LeaderContentId { get; set; }
    public string LeaderName { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }
    public List<ulong> MemberContentIds { get; set; } = new();
}
