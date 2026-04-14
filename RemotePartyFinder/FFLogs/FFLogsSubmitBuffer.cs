using System;
using System.Collections.Generic;
using System.Linq;

namespace RemotePartyFinder;

internal sealed class FFLogsSubmitBuffer
{
    private readonly Dictionary<string, ParseResult> _pendingSubmitResults = new(StringComparer.Ordinal);

    internal static string GetParseResultKey(ParseResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return $"{result.ContentId}:{result.ZoneId}:{result.DifficultyId}:{result.Partition}";
    }

    public void QueueSubmitResults(IEnumerable<ParseResult> freshResults)
    {
        ArgumentNullException.ThrowIfNull(freshResults);

        foreach (var result in freshResults)
        {
            _pendingSubmitResults[GetParseResultKey(result)] = result;
        }
    }

    public List<ParseResult> BuildSubmitBatch(List<ParseResult> freshResults)
    {
        ArgumentNullException.ThrowIfNull(freshResults);

        QueueSubmitResults(freshResults);
        if (_pendingSubmitResults.Count == 0)
        {
            return [];
        }

        var batch = _pendingSubmitResults.Values.ToList();
        _pendingSubmitResults.Clear();
        return batch;
    }

    public void RequeueSubmitBatch(IEnumerable<ParseResult> failedBatch)
    {
        ArgumentNullException.ThrowIfNull(failedBatch);

        foreach (var result in failedBatch)
        {
            _pendingSubmitResults[GetParseResultKey(result)] = result;
        }
    }
}
