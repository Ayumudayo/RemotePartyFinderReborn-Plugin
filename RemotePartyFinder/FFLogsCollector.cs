using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace RemotePartyFinder;

internal interface IFFLogsIngestHttpSender
{
    Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken);
}

internal interface IFFLogsApiClient
{
    bool TryGetRateLimitRemaining(out TimeSpan remaining);
    DateTime RateLimitCooldownUntilUtc { get; }
    void ResetRateLimitCooldown();
    Task<Dictionary<string, FFLogsClient.CharacterFetchedData>> FetchCharacterCandidateDataBatchAsync(
        List<FFLogsClient.CandidateCharacterQuery> queries,
        int zoneId,
        int? difficultyId,
        int? partition,
        int recentReportsLimit,
        CancellationToken cancellationToken);
    Task<Dictionary<string, double>> FetchBestBossPercentByReportAsync(
        List<string> reportCodes,
        int encounterId,
        int? difficultyId,
        CancellationToken cancellationToken);
}

internal interface IFFLogsTimeProvider
{
    DateTime UtcNow { get; }
}

internal sealed record FFLogsCollectorSeams(
    IFFLogsIngestHttpSender IngestHttpSender,
    IFFLogsApiClient ApiClient,
    IFFLogsTimeProvider TimeProvider);

public class FFLogsCollector : IDisposable
{
    private sealed class HttpClientIngestHttpSender(HttpClient httpClient) : IFFLogsIngestHttpSender
    {
        public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => httpClient.SendAsync(request, cancellationToken);
    }

    private sealed class FFLogsApiClientAdapter(FFLogsClient client) : IFFLogsApiClient
    {
        public bool TryGetRateLimitRemaining(out TimeSpan remaining)
            => client.TryGetRateLimitRemaining(out remaining);

        public DateTime RateLimitCooldownUntilUtc
            => client.RateLimitCooldownUntilUtc;

        public void ResetRateLimitCooldown()
            => client.ResetRateLimitCooldown();

        public Task<Dictionary<string, FFLogsClient.CharacterFetchedData>> FetchCharacterCandidateDataBatchAsync(
            List<FFLogsClient.CandidateCharacterQuery> queries,
            int zoneId,
            int? difficultyId,
            int? partition,
            int recentReportsLimit,
            CancellationToken cancellationToken)
            => client.FetchCharacterCandidateDataBatchAsync(
                queries,
                zoneId,
                difficultyId,
                partition,
                recentReportsLimit,
                cancellationToken);

        public Task<Dictionary<string, double>> FetchBestBossPercentByReportAsync(
            List<string> reportCodes,
            int encounterId,
            int? difficultyId,
            CancellationToken cancellationToken)
            => client.FetchBestBossPercentByReportAsync(
                reportCodes,
                encounterId,
                difficultyId,
                cancellationToken);
    }

    private sealed class SystemFFLogsTimeProvider : IFFLogsTimeProvider
    {
        public DateTime UtcNow
            => DateTime.UtcNow;
    }

    private Configuration Configuration { get; set; }
    private Action<string> InfoLog { get; set; } = static _ => { };
    private Action<string> WarningLog { get; set; } = static _ => { };
    private Action<string> ErrorLog { get; set; } = static _ => { };
    private Action<string> DebugLog { get; set; } = static _ => { };
    private FFLogsCollectorSeams Seams { get; set; }
    private FFLogsSubmitBuffer SubmitBuffer { get; set; }
    private FFLogsWorkerPolicy WorkerPolicy { get; set; }
    private FFLogsResultSubmitter ResultSubmitter { get; set; }
    private FFLogsLeaseAbandoner LeaseAbandoner { get; set; }
    private FFLogsJobLeaseClient JobLeaseClient { get; set; }
    private FFLogsBatchProcessor BatchProcessor { get; set; }
    private FFLogsBackgroundWorker BackgroundWorker { get; set; }
    private IReadOnlyList<IDisposable> OwnedDisposables { get; set; } = [];
    private System.Threading.CancellationTokenSource Cts { get; set; } = new();

    internal static FFLogsCollectorSeams CreateSeams(
        IFFLogsIngestHttpSender ingestHttpSender,
        IFFLogsApiClient apiClient,
        IFFLogsTimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(ingestHttpSender);
        ArgumentNullException.ThrowIfNull(apiClient);
        ArgumentNullException.ThrowIfNull(timeProvider);
        return new FFLogsCollectorSeams(ingestHttpSender, apiClient, timeProvider);
    }

    internal static FFLogsCollector CreateForTesting(
        Configuration configuration,
        FFLogsCollectorSeams seams,
        FFLogsSubmitBuffer submitBuffer,
        FFLogsWorkerPolicy workerPolicy,
        FFLogsJobLeaseClient jobLeaseClient = null,
        FFLogsResultSubmitter resultSubmitter = null,
        FFLogsBatchProcessor batchProcessor = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(seams);
        ArgumentNullException.ThrowIfNull(submitBuffer);
        ArgumentNullException.ThrowIfNull(workerPolicy);

        var collector = new FFLogsCollector();
        collector.Initialize(
            configuration,
            static _ => { },
            static _ => { },
            static _ => { },
            static _ => { },
            seams,
            submitBuffer,
            workerPolicy,
            resultSubmitter ?? new FFLogsResultSubmitter(seams, submitBuffer),
            new FFLogsLeaseAbandoner(seams),
            jobLeaseClient ?? new FFLogsJobLeaseClient(seams),
            batchProcessor ?? new FFLogsBatchProcessor(seams),
            [],
            startWorker: false);
        return collector;
    }

    internal Task RunWorkerLoopForTestingAsync()
        => BackgroundWorker.RunAsync(Cts.Token);

    private FFLogsCollector()
    {
    }

    public FFLogsCollector(Plugin plugin)
    {
        ArgumentNullException.ThrowIfNull(plugin);

        var httpClient = new HttpClient();
        var ffLogsClient = new FFLogsClient(plugin.Configuration);
        var seams = CreateSeams(
            new HttpClientIngestHttpSender(httpClient),
            new FFLogsApiClientAdapter(ffLogsClient),
            new SystemFFLogsTimeProvider());
        var submitBuffer = new FFLogsSubmitBuffer();
        Initialize(
            plugin.Configuration,
            message => Plugin.Log.Info(message),
            message => Plugin.Log.Warning(message),
            message => Plugin.Log.Error(message),
            message => Plugin.Log.Debug(message),
            seams,
            submitBuffer,
            new FFLogsWorkerPolicy(plugin.Configuration, message => Plugin.Log.Warning(message), seams.TimeProvider),
            new FFLogsResultSubmitter(seams, submitBuffer),
            new FFLogsLeaseAbandoner(seams),
            new FFLogsJobLeaseClient(seams),
            new FFLogsBatchProcessor(seams),
            [ffLogsClient, httpClient],
            startWorker: true);
    }

    public void Dispose()
    {
        Cts.Cancel();
        foreach (var disposable in OwnedDisposables)
        {
            disposable.Dispose();
        }
    }

    private void Initialize(
        Configuration configuration,
        Action<string> infoLog,
        Action<string> warningLog,
        Action<string> errorLog,
        Action<string> debugLog,
        FFLogsCollectorSeams seams,
        FFLogsSubmitBuffer submitBuffer,
        FFLogsWorkerPolicy workerPolicy,
        FFLogsResultSubmitter resultSubmitter,
        FFLogsLeaseAbandoner leaseAbandoner,
        FFLogsJobLeaseClient jobLeaseClient,
        FFLogsBatchProcessor batchProcessor,
        IReadOnlyList<IDisposable> ownedDisposables,
        bool startWorker)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        InfoLog = infoLog ?? throw new ArgumentNullException(nameof(infoLog));
        WarningLog = warningLog ?? throw new ArgumentNullException(nameof(warningLog));
        ErrorLog = errorLog ?? throw new ArgumentNullException(nameof(errorLog));
        DebugLog = debugLog ?? throw new ArgumentNullException(nameof(debugLog));
        Seams = seams ?? throw new ArgumentNullException(nameof(seams));
        SubmitBuffer = submitBuffer ?? throw new ArgumentNullException(nameof(submitBuffer));
        WorkerPolicy = workerPolicy ?? throw new ArgumentNullException(nameof(workerPolicy));
        ResultSubmitter = resultSubmitter ?? throw new ArgumentNullException(nameof(resultSubmitter));
        LeaseAbandoner = leaseAbandoner ?? throw new ArgumentNullException(nameof(leaseAbandoner));
        JobLeaseClient = jobLeaseClient ?? throw new ArgumentNullException(nameof(jobLeaseClient));
        BatchProcessor = batchProcessor ?? throw new ArgumentNullException(nameof(batchProcessor));
        OwnedDisposables = ownedDisposables ?? throw new ArgumentNullException(nameof(ownedDisposables));
        BackgroundWorker = new FFLogsBackgroundWorker(
            Configuration,
            Seams.ApiClient,
            WorkerPolicy,
            JobLeaseClient,
            BatchProcessor,
            ResultSubmitter,
            LeaseAbandoner,
            InfoLog,
            WarningLog,
            ErrorLog,
            DebugLog);
        if (startWorker)
        {
            StartWorker();
        }
    }

    private void StartWorker()
    {
        Task.Run(() => BackgroundWorker.RunAsync(Cts.Token), Cts.Token);
    }

    public bool TryGetRateLimitCooldownRemaining(out TimeSpan remaining)
        => Seams.ApiClient.TryGetRateLimitRemaining(out remaining);

    public DateTime RateLimitCooldownUntilUtc
        => Seams.ApiClient.RateLimitCooldownUntilUtc;

    public void ResetRateLimitCooldown()
        => Seams.ApiClient.ResetRateLimitCooldown();
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseJob
{
    public ulong ContentId { get; set; }
    public string Name { get; set; } = "";
    public string Server { get; set; } = "";
    public string Region { get; set; } = "";
    public List<ParseJobCandidateServer> CandidateServers { get; set; } = new();
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
    public uint EncounterId { get; set; }
    public uint? SecondaryEncounterId { get; set; }
    public string LeaseToken { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseJobCandidateServer
{
    public string Server { get; set; } = "";
    public string Region { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ParseResult
{
    public ulong ContentId { get; set; }
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
    public Dictionary<int, double> Encounters { get; set; } = new();
    public Dictionary<int, double> BossPercentages { get; set; } = new();
    public Dictionary<int, int> ClearCounts { get; set; } = new();
    public bool IsHidden { get; set; }
    public bool IsEstimated { get; set; }
    public string MatchedServer { get; set; } = "";
    public string LeaseToken { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class AbandonFflogsLease
{
    public ulong ContentId { get; set; }
    public uint ZoneId { get; set; }
    public int DifficultyId { get; set; }
    public int Partition { get; set; }
    public string LeaseToken { get; set; } = "";
    public string Reason { get; set; } = "";
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ContributeFflogsResultsResponse
{
    public string Status { get; set; } = "";
    public int Submitted { get; set; }
    public int Accepted { get; set; }
    public int Updated { get; set; }
    public int Rejected { get; set; }
}

[Serializable]
[JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
public class ContributeFflogsLeaseAbandonResponse
{
    public string Status { get; set; } = "";
    public int Submitted { get; set; }
    public int Released { get; set; }
    public int Rejected { get; set; }
}
