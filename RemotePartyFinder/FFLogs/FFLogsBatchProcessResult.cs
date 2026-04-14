using System;
using System.Collections.Generic;

namespace RemotePartyFinder;

internal sealed record FFLogsBatchProcessResult(
    IReadOnlyList<ParseResult> ProcessedResults,
    bool HadTransientFailure,
    bool HitRateLimitCooldown,
    TimeSpan CooldownRemaining,
    bool ShouldAbandonRemainingLeases);
