using System;
using System.Threading;
using System.Threading.Tasks;

namespace RemotePartyFinder;

internal sealed class FFLogsWorkerPolicy
{
    private readonly Configuration _configuration;
    private readonly Action<string> _warningSink;
    private readonly IFFLogsTimeProvider _timeProvider;
    private readonly Func<int, CancellationToken, Task> _delayAsync;
    private DateTime _lastCooldownSkipLogUtc = DateTime.MinValue;

    public FFLogsWorkerPolicy(
        Configuration configuration,
        Action<string> warningSink,
        IFFLogsTimeProvider timeProvider,
        Func<int, CancellationToken, Task> delayAsync = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _warningSink = warningSink ?? throw new ArgumentNullException(nameof(warningSink));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _delayAsync = delayAsync ?? DefaultDelayAsync;
    }

    public int WorkerBaseDelayMs
        => Math.Clamp(_configuration.FFLogsWorkerBaseDelayMs, 1000, 120000);

    public int WorkerIdleDelayMs
        => Math.Clamp(_configuration.FFLogsWorkerIdleDelayMs, 1000, 300000);

    public int WorkerJitterMs
        => Math.Clamp(_configuration.FFLogsWorkerJitterMs, 0, 30000);

    public int WorkerMaxBackoffDelayMs
        => Math.Clamp(
            _configuration.FFLogsWorkerMaxBackoffDelayMs,
            WorkerBaseDelayMs,
            600000);

    public int LastBackoffDelayMs { get; private set; }

    public int ComputeIdleDelayMs()
        => WorkerIdleDelayMs;

    public void LogCooldownSkipIfNeeded(TimeSpan remaining)
    {
        var now = _timeProvider.UtcNow;
        if ((now - _lastCooldownSkipLogUtc) < TimeSpan.FromMinutes(1))
        {
            return;
        }

        _lastCooldownSkipLogUtc = now;
        var minutesRemaining = Math.Max(1, (int)Math.Ceiling(remaining.TotalMinutes));
        _warningSink($"FFLogsCollector: FFLogs cooldown active; skipping server polling for about {minutesRemaining} minute(s).");
    }

    public int ComputeNextBackoffFailures(int consecutiveFailures)
        => Math.Min(consecutiveFailures + 1, 16);

    public int ComputeBackoffDelayMs(int consecutiveFailures)
    {
        var nextFailures = ComputeNextBackoffFailures(consecutiveFailures);
        var exponent = Math.Min(nextFailures - 1, 4);
        return Math.Min(WorkerMaxBackoffDelayMs, WorkerBaseDelayMs * (1 << exponent));
    }

    public Task DelayAsync(int baseDelayMs, CancellationToken cancellationToken)
        => _delayAsync(ComputeJitteredDelayMs(baseDelayMs), cancellationToken);

    public async Task<int> DelayWithBackoffAsync(int consecutiveFailures, CancellationToken cancellationToken)
    {
        LastBackoffDelayMs = ComputeBackoffDelayMs(consecutiveFailures);
        await DelayAsync(LastBackoffDelayMs, cancellationToken);
        return ComputeNextBackoffFailures(consecutiveFailures);
    }

    private int ComputeJitteredDelayMs(int baseDelayMs)
    {
        if (baseDelayMs < 0)
        {
            baseDelayMs = 0;
        }

        return baseDelayMs + Random.Shared.Next(0, WorkerJitterMs + 1);
    }

    private static Task DefaultDelayAsync(int delayMs, CancellationToken cancellationToken)
        => Task.Delay(delayMs, cancellationToken);
}
