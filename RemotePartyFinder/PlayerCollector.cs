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
/// PlayerDataCache를 통해 미전송 데이터를 암호화하여 로컬에 캐시합니다.
/// </summary>
internal class PlayerCollector : IDisposable {
    private Plugin Plugin { get; }
    private PlayerDataCache Cache { get; }
    private HttpClient Client { get; } = new();
    private Stopwatch ScanTimer { get; } = new();
    
    // 이미 수집한 플레이어를 캐시하여 중복 수집 방지
    private ConcurrentDictionary<ulong, DateTime> CollectedPlayers { get; } = new();
    private volatile bool _isUploading;
    private const int CacheExpirationMinutes = 5;
    private const int ScanIntervalSeconds = 5;
    
    // FFXIVClientStructs Character 구조체의 ContentId 오프셋 (0x2358)
    private const int ContentIdOffset = 0x2358;
    // FFXIVClientStructs Character 구조체의 AccountId 오프셋 (0x2350)
    private const int AccountIdOffset = 0x2350;

    internal PlayerCollector(Plugin plugin) {
        this.Plugin = plugin;
        this.Cache = new PlayerDataCache(plugin);
        this.ScanTimer.Start();
        this.Plugin.Framework.Update += this.OnUpdate;
    }

    public void Dispose() {
        this.Plugin.Framework.Update -= this.OnUpdate;
        this.Cache.Dispose();
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
        var newPlayers = new List<UploadablePlayer>();
        var now = DateTime.UtcNow;

        foreach (var obj in this.Plugin.ObjectTable) {
            if (obj.ObjectKind != ObjectKind.Player) continue;
            if (obj is not IPlayerCharacter playerCharacter) continue;
            
            // ContentId 읽기 (직접 메모리 접근)
            var contentId = (ulong)Marshal.ReadInt64(obj.Address + ContentIdOffset);
            var accountId = (ulong)Marshal.ReadInt64(obj.Address + AccountIdOffset);
            var homeWorld = (ushort)playerCharacter.HomeWorld.RowId;
            var currentWorld = (ushort)playerCharacter.CurrentWorld.RowId;
            var name = obj.Name.TextValue;
            
            // 유효성 검사
            if (contentId == 0 || homeWorld == 0 || homeWorld >= 1000 || string.IsNullOrEmpty(name)) continue;
            
            // 캐시 확인 (최근에 수집한 플레이어는 스킵)
            if (this.CollectedPlayers.TryGetValue(contentId, out var lastCollect)) {
                if ((now - lastCollect).TotalMinutes < CacheExpirationMinutes) continue;
            }
            
            newPlayers.Add(new UploadablePlayer {
                ContentId = contentId,
                Name = name,
                HomeWorld = homeWorld,
                CurrentWorld = currentWorld,
                AccountId = accountId,
            });
            
            // 수집 캐시에 추가
            this.CollectedPlayers[contentId] = now;
        }

        // 오래된 캐시 정리
        var expiredKeys = this.CollectedPlayers
            .Where(kvp => (now - kvp.Value).TotalMinutes > CacheExpirationMinutes * 2)
            .Select(kvp => kvp.Key)
            .ToList();
        foreach (var key in expiredKeys) {
            this.CollectedPlayers.TryRemove(key, out _);
        }

        // 새로 수집된 플레이어를 캐시 큐에 추가
        if (newPlayers.Count > 0) {
            this.Cache.EnqueueRange(newPlayers);
        }

        // 캐시에 대기 중인 데이터가 있으면 업로드 시도
        if (this.Cache.PendingCount > 0) {
            this._isUploading = true;
            UploadFromCacheAsync();
        }
    }

    private void UploadFromCacheAsync() {
        Task.Run(async () => {
            try {
                // 캐시에서 최대 100개 가져오기
                var batch = this.Cache.TakeBatch();
                if (batch.Count == 0) {
                    return;
                }

                var json = JsonConvert.SerializeObject(batch);
                var success = false;

                foreach (var uploadUrl in this.Plugin.Configuration.UploadUrls.Where(u => u.IsEnabled)) {
                    // Circuit Breaker
                    if (uploadUrl.FailureCount >= this.Plugin.Configuration.CircuitBreakerFailureThreshold) {
                        if ((DateTime.UtcNow - uploadUrl.LastFailureTime).TotalMinutes < this.Plugin.Configuration.CircuitBreakerBreakDurationMinutes) {
                            continue;
                        }
                    }

                    var baseUrl = uploadUrl.Url.TrimEnd('/');
                    
                    // Base URL 정리
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
                            success = true;
                            uploadUrl.FailureCount = 0;
                        } else {
                            uploadUrl.FailureCount++;
                            uploadUrl.LastFailureTime = DateTime.UtcNow;
                        }
                        
                        var output = await resp.Content.ReadAsStringAsync();
                        Plugin.Log.Debug($"PlayerCollector: {playersUrl}: {resp.StatusCode} ({batch.Count} players)");
                    } catch (Exception ex) {
                        uploadUrl.FailureCount++;
                        uploadUrl.LastFailureTime = DateTime.UtcNow;
                        Plugin.Log.Error($"PlayerCollector upload error to {playersUrl}: {ex.Message}");
                    }
                }

                // 실패 시 배치를 캐시에 다시 추가
                if (!success) {
                    this.Cache.ReturnBatch(batch);
                    Plugin.Log.Warning($"PlayerCollector: Upload failed, {batch.Count} players returned to cache. Total pending: {this.Cache.PendingCount}");
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
    [JsonProperty(DefaultValueHandling = DefaultValueHandling.Include)]
    public ushort CurrentWorld { get; set; }
    public ulong AccountId { get; set; }
}
