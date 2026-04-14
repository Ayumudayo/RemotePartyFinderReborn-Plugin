using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace RemotePartyFinder.Tests;

internal sealed class ManualFFLogsTimeProvider : IFFLogsTimeProvider {
    public DateTime UtcNow { get; set; }
}

internal sealed class RecordingFFLogsWarningSink {
    public List<string> Messages { get; } = [];

    public void Warning(string message) {
        Messages.Add(message);
    }
}

internal sealed class StubFFLogsIngestHttpSender : IFFLogsIngestHttpSender {
    public List<HttpRequestMessage> Requests { get; } = [];

    public Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> OnSendAsync { get; set; }
        = static (_, cancellationToken) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));

    public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
        Requests.Add(request);
        return OnSendAsync(request, cancellationToken);
    }
}

internal sealed class StubFFLogsApiClient : IFFLogsApiClient {
    public Func<(bool HasCooldown, TimeSpan Remaining)> OnTryGetRateLimitRemaining { get; set; }
        = static () => (false, TimeSpan.Zero);

    public Func<List<FFLogsClient.CandidateCharacterQuery>, int, int?, int?, int, CancellationToken, Task<Dictionary<string, FFLogsClient.CharacterFetchedData>>> OnFetchCharacterCandidateDataBatchAsync { get; set; }
        = static (_, _, _, _, _, _) => Task.FromResult(new Dictionary<string, FFLogsClient.CharacterFetchedData>());

    public Func<List<string>, int, int?, CancellationToken, Task<Dictionary<string, double>>> OnFetchBestBossPercentByReportAsync { get; set; }
        = static (_, _, _, _) => Task.FromResult(new Dictionary<string, double>());

    public DateTime RateLimitCooldownUntilUtc { get; set; }

    public bool TryGetRateLimitRemaining(out TimeSpan remaining) {
        var result = OnTryGetRateLimitRemaining();
        remaining = result.Remaining;
        return result.HasCooldown;
    }

    public void ResetRateLimitCooldown() {
        RateLimitCooldownUntilUtc = DateTime.MinValue;
    }

    public Task<Dictionary<string, FFLogsClient.CharacterFetchedData>> FetchCharacterCandidateDataBatchAsync(
        List<FFLogsClient.CandidateCharacterQuery> queries,
        int zoneId,
        int? difficultyId,
        int? partition,
        int recentReportsLimit,
        CancellationToken cancellationToken) {
        return OnFetchCharacterCandidateDataBatchAsync(
            queries,
            zoneId,
            difficultyId,
            partition,
            recentReportsLimit,
            cancellationToken);
    }

    public Task<Dictionary<string, double>> FetchBestBossPercentByReportAsync(
        List<string> reportCodes,
        int encounterId,
        int? difficultyId,
        CancellationToken cancellationToken) {
        return OnFetchBestBossPercentByReportAsync(
            reportCodes,
            encounterId,
            difficultyId,
            cancellationToken);
    }
}

internal static class FFLogsTestAssemblyResolver {
    private static int _registered;

    public static void Register() {
        if (Interlocked.Exchange(ref _registered, 1) != 0) {
            return;
        }

        AppDomain.CurrentDomain.AssemblyResolve += static (_, args) => {
            var assemblyName = new AssemblyName(args.Name).Name;
            if (string.IsNullOrWhiteSpace(assemblyName)) {
                return null;
            }

            var dalamudHome = Environment.GetEnvironmentVariable("DALAMUD_HOME");
            if (string.IsNullOrWhiteSpace(dalamudHome)) {
                dalamudHome = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                    "XIVLauncher",
                    "addon",
                    "Hooks",
                    "dev"
                );
            }

            var candidatePath = Path.Combine(dalamudHome, assemblyName + ".dll");
            return File.Exists(candidatePath) ? Assembly.LoadFrom(candidatePath) : null;
        };
    }
}
