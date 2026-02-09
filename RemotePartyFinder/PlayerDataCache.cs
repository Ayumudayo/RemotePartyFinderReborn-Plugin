using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

namespace RemotePartyFinder;

/// <summary>
/// 플레이어 데이터를 암호화하여 로컬에 캐시하고 배치 전송을 관리합니다.
/// </summary>
internal class PlayerDataCache : IDisposable
{
    private readonly Plugin _plugin;
    private readonly string _cacheFilePath;
    private readonly ConcurrentQueue<UploadablePlayer> _pendingQueue = new();
    
    /// <summary>
    /// 1회 전송 시 최대 플레이어 수
    /// </summary>
    public const int MaxBatchSize = 100;
    
    /// <summary>
    /// 대기 중인 플레이어 수
    /// </summary>
    public int PendingCount => _pendingQueue.Count;

    public PlayerDataCache(Plugin plugin)
    {
        _plugin = plugin;
        _cacheFilePath = Path.Combine(
            Plugin.PluginInterface.ConfigDirectory.FullName,
            "player_cache.enc"
        );
        
        LoadFromDisk();
    }

    public void Dispose()
    {
        SaveToDisk();
    }

    /// <summary>
    /// 플레이어를 캐시 큐에 추가합니다.
    /// </summary>
    public void Enqueue(UploadablePlayer player)
    {
        _pendingQueue.Enqueue(player);
    }

    /// <summary>
    /// 플레이어 목록을 캐시 큐에 추가합니다.
    /// </summary>
    public void EnqueueRange(IEnumerable<UploadablePlayer> players)
    {
        foreach (var player in players)
        {
            _pendingQueue.Enqueue(player);
        }
    }

    /// <summary>
    /// 전송할 배치를 가져옵니다 (최대 MaxBatchSize).
    /// 호출 시 큐에서 제거됩니다.
    /// </summary>
    public List<UploadablePlayer> TakeBatch()
    {
        var batch = new List<UploadablePlayer>();
        
        while (batch.Count < MaxBatchSize && _pendingQueue.TryDequeue(out var player))
        {
            batch.Add(player);
        }
        
        return batch;
    }

    /// <summary>
    /// 전송 실패 시 배치를 큐 앞쪽에 다시 추가합니다.
    /// </summary>
    public void ReturnBatch(List<UploadablePlayer> batch)
    {
        // ConcurrentQueue는 앞에 추가 불가하므로, 새 큐 생성 후 병합
        var tempList = batch.ToList();
        while (_pendingQueue.TryDequeue(out var existing))
        {
            tempList.Add(existing);
        }
        
        foreach (var player in tempList)
        {
            _pendingQueue.Enqueue(player);
        }
        
        // 실패 시 즉시 디스크에 저장
        SaveToDisk();
    }

    /// <summary>
    /// 캐시를 디스크에 암호화하여 저장합니다.
    /// </summary>
    public void SaveToDisk()
    {
        try
        {
            var players = _pendingQueue.ToArray();
            if (players.Length == 0)
            {
                // 빈 캐시면 파일 삭제
                if (File.Exists(_cacheFilePath))
                {
                    File.Delete(_cacheFilePath);
                }
                return;
            }

            var json = JsonConvert.SerializeObject(players);
            var encrypted = Encrypt(json);
            File.WriteAllBytes(_cacheFilePath, encrypted);
            
            Plugin.Log.Debug($"PlayerDataCache: Saved {players.Length} players to disk.");
        }
        catch (Exception ex)
        {
            Plugin.Log.Error($"PlayerDataCache: Failed to save cache: {ex.Message}");
        }
    }

    /// <summary>
    /// 디스크에서 암호화된 캐시를 로드합니다.
    /// </summary>
    private void LoadFromDisk()
    {
        try
        {
            if (!File.Exists(_cacheFilePath))
            {
                return;
            }

            var encrypted = File.ReadAllBytes(_cacheFilePath);
            var json = Decrypt(encrypted);
            var players = JsonConvert.DeserializeObject<UploadablePlayer[]>(json);

            if (players != null)
            {
                foreach (var player in players)
                {
                    _pendingQueue.Enqueue(player);
                }
                Plugin.Log.Info($"PlayerDataCache: Loaded {players.Length} players from disk.");
            }
        }
        catch (Exception ex)
        {
            Plugin.Log.Error($"PlayerDataCache: Failed to load cache: {ex.Message}");
            // 손상된 캐시 파일 삭제
            try { File.Delete(_cacheFilePath); } catch { }
        }
    }

    #region Encryption (DPAPI)

    /// <summary>
    /// DPAPI를 사용하여 데이터를 암호화합니다.
    /// Windows 사용자 계정에 바인딩되어 해당 계정에서만 복호화 가능합니다.
    /// </summary>
    private static byte[] Encrypt(string plainText)
    {
        var plainBytes = Encoding.UTF8.GetBytes(plainText);
        return ProtectedData.Protect(plainBytes, null, DataProtectionScope.CurrentUser);
    }

    /// <summary>
    /// DPAPI를 사용하여 데이터를 복호화합니다.
    /// </summary>
    private static string Decrypt(byte[] encryptedBytes)
    {
        var plainBytes = ProtectedData.Unprotect(encryptedBytes, null, DataProtectionScope.CurrentUser);
        return Encoding.UTF8.GetString(plainBytes);
    }

    #endregion
}
