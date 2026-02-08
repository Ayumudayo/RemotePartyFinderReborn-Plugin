using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Dalamud.Game.ClientState.Objects.Enums;
using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

/// <summary>
/// ObjectTable을 주기적으로 스캔하여 플레이어 정보를 수집합니다.
/// </summary>
internal class PlayerCollector : IDisposable {
    private Plugin Plugin { get; }
    private HttpClient Client { get; } = new();
    private Stopwatch ScanTimer { get; } = new();
    
    // 이미 업로드한 플레이어를 캐시하여 중복 업로드 방지
    private ConcurrentDictionary<ulong, DateTime> UploadedPlayers { get; } = new();
    private volatile bool _isUploading;
    private const int CacheExpirationMinutes = 5;
    private const int ScanIntervalSeconds = 5;
    
    // FFXIVClientStructs Character 구조체의 ContentId 오프셋 (0x2358)
    private const int ContentIdOffset = 0x2358;

    internal PlayerCollector(Plugin plugin) {
        this.Plugin = plugin;
        this.ScanTimer.Start();
        this.Plugin.Framework.Update += this.OnUpdate;
    }

    public void Dispose() {
        this.Plugin.Framework.Update -= this.OnUpdate;
    }

    private void OnUpdate(IFramework framework) {
        // 스캔 주기 체크
        if (this.ScanTimer.Elapsed < TimeSpan.FromSeconds(ScanIntervalSeconds)) {
            return;
        }
        this.ScanTimer.Restart();


        // 업로드 중이면 스킵 (Stacking 방지)
        if (this._isUploading) return;

        // ObjectTable에서 플레이어 수집
        var players = new List<UploadablePlayer>();
        var now = DateTime.UtcNow;

        foreach (var obj in this.Plugin.ObjectTable) {
            if (obj.ObjectKind != ObjectKind.Player) continue;
            if (obj is not IPlayerCharacter playerCharacter) continue;
            
            // ContentId 읽기 (직접 메모리 접근)
            var contentId = (ulong)Marshal.ReadInt64(obj.Address + ContentIdOffset);
            var homeWorld = (ushort)playerCharacter.HomeWorld.RowId;
            var name = obj.Name.TextValue;
            
            // 유효성 검사
            if (contentId == 0 || homeWorld == 0 || homeWorld >= 1000 || string.IsNullOrEmpty(name)) continue;
            
            // 캐시 확인 (최근에 업로드한 플레이어는 스킵)
            if (this.UploadedPlayers.TryGetValue(contentId, out var lastUpload)) {
                if ((now - lastUpload).TotalMinutes < CacheExpirationMinutes) continue;
            }
            
            players.Add(new UploadablePlayer {
                ContentId = contentId,
                Name = name,
                HomeWorld = homeWorld,
            });
            

        }

        // 오래된 캐시 정리
        var expiredKeys = this.UploadedPlayers
            .Where(kvp => (now - kvp.Value).TotalMinutes > CacheExpirationMinutes * 2)
            .Select(kvp => kvp.Key)
            .ToList();
        foreach (var key in expiredKeys) {
            this.UploadedPlayers.TryRemove(key, out _);
        }

        // 서버에 업로드 (별도 safe 컨텍스트에서)
        if (players.Count > 0) {
            this._isUploading = true;
            UploadPlayersAsync(players);
        }
    }

    private void UploadPlayersAsync(List<UploadablePlayer> players) {
        Task.Run(async () => {
            try {
                var json = JsonConvert.SerializeObject(players);
                var success = false;

                foreach (var uploadUrl in this.Plugin.Configuration.UploadUrls.Where(u => u.IsEnabled)) {
                    // Circuit Breaker
                    if (uploadUrl.FailureCount >= this.Plugin.Configuration.CircuitBreakerFailureThreshold) {
                        if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < this.Plugin.Configuration.CircuitBreakerBreakDurationMinutes) {
                            continue;
                        }
                    }

                    var baseUrl = uploadUrl.Url.TrimEnd('/');
                    
                    // Base URL 정리: /contribute/multiple 또는 /contribute 로 끝나면 해당 부분 제거
                    if (baseUrl.EndsWith("/contribute/multiple")) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute/multiple".Length);
                    } else if (baseUrl.EndsWith("/contribute")) {
                        baseUrl = baseUrl.Substring(0, baseUrl.Length - "/contribute".Length);
                    }
                    
                    var playersUrl = baseUrl + "/contribute/players";
                    
                    try {
                        var resp = await this.Client.PostAsync(playersUrl, new StringContent(json) {
                            Headers = { ContentType = MediaTypeHeaderValue.Parse("application/json") },
                        });
                        
                        if (resp.IsSuccessStatusCode) {
                            success = true; // 적어도 하나의 서버에 성공함
                            uploadUrl.FailureCount = 0;
                        } else {
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                        }
                        
                        var output = await resp.Content.ReadAsStringAsync();
                        Plugin.Log.Debug($"PlayerCollector: {playersUrl}: {resp.StatusCode} {output}");
                    } catch (Exception ex) {
                        uploadUrl.FailureCount++;
                        uploadUrl.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"PlayerCollector upload error to {playersUrl}: {ex.Message}");
                    }
                }

                // 하나라도 성공했으면 캐시 업데이트
                if (success) {
                    var now = DateTime.UtcNow;
                    foreach (var player in players) {
                        this.UploadedPlayers[player.ContentId] = now;
                    }
                }
            } catch (Exception e) {
                Plugin.Log.Error($"PlayerCollector upload error: {e.Message}");
            } finally {
                this._isUploading = false;
            }
        });
    }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
internal class UploadablePlayer {
    public ulong ContentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public ushort HomeWorld { get; set; }
}
