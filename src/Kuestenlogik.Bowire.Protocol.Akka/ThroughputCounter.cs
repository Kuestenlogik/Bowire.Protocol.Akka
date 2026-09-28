// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// Messages per actor path, counted at the tap itself (#29).
/// </summary>
/// <remarks>
/// <para>
/// Counting happens in <see cref="BowireAkkaExtension.Publish"/>, not by
/// reading the tap stream: that stream is a bounded channel that drops the
/// oldest entries when a reader falls behind, so a counter fed from it would
/// undercount exactly when the numbers matter — under load. Here every
/// enqueue a tap sees is one increment, whether or not anybody reads the
/// messages.
/// </para>
/// <para>
/// A counter exists only while somebody watches the throughput. With none
/// registered the tap costs what it did before: one check per enqueue.
/// </para>
/// </remarks>
public sealed class ThroughputCounter
{
    // A boxed long per path so the hot path is one dictionary lookup and an
    // Interlocked.Increment, not an AddOrUpdate allocating a closure.
    private sealed class Cell { public long Value; }

    private readonly ConcurrentDictionary<string, Cell> _cells = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _totals = new(StringComparer.Ordinal);

    internal void Record(string path)
    {
        var cell = _cells.GetOrAdd(path, static _ => new Cell());
        Interlocked.Increment(ref cell.Value);
    }

    /// <summary>
    /// The counts since the previous call, per actor path, and the running
    /// total since the counter was started. Called by one reader at a time.
    /// </summary>
    public IReadOnlyList<(string Path, long Messages, long Total)> Drain()
    {
        var result = new List<(string, long, long)>();
        foreach (var (path, cell) in _cells)
        {
            // Take the count and reset it in one step: an increment racing
            // this lands before (counted now) or after (counted next time) —
            // never lost, never counted twice.
            var n = Interlocked.Exchange(ref cell.Value, 0);
            if (n == 0) continue;
            var total = (_totals.TryGetValue(path, out var t) ? t : 0) + n;
            _totals[path] = total;
            result.Add((path, n, total));
        }
        return result;
    }
}

/// <summary>One actor's throughput over one interval.</summary>
/// <param name="Path">Absolute actor path (the dead-letter path for dead letters).</param>
/// <param name="Messages">Messages enqueued during the interval.</param>
/// <param name="PerSecond">Messages per second over the interval.</param>
/// <param name="Total">Messages since the throughput view was opened.</param>
public sealed record ActorThroughput(string Path, long Messages, double PerSecond, long Total);

/// <summary>One tick of the throughput stream.</summary>
/// <param name="Timestamp">UTC end of the interval.</param>
/// <param name="IntervalSeconds">Measured length of the interval.</param>
/// <param name="Actors">The actors that received messages in it, busiest first.</param>
public sealed record ThroughputSnapshot(DateTime Timestamp, double IntervalSeconds, IReadOnlyList<ActorThroughput> Actors);
