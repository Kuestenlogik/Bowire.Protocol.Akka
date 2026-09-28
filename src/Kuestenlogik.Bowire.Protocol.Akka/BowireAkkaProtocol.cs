// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Runtime.CompilerServices;
using System.Text.Json;
using Akka.Actor;
using Kuestenlogik.Bowire.Models;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// Bowire protocol plugin for Akka.NET actor systems. Surfaces a single
/// "Tap" service whose only method, <c>MonitorMessages</c>, is a server-
/// streaming subscription to every message that lands in a tap-mailboxed
/// actor's mailbox (see <see cref="BowireTapMailbox"/>).
/// <para>
/// Current scope (1.0.x): embedded mode only — the plugin grabs the
/// host app's <see cref="ActorSystem"/> from DI and reads from
/// <see cref="BowireAkkaExtension"/>. Standalone CLI via
/// Akka.Cluster.Tools.ClusterClient is planned for 1.1.0.
/// </para>
/// </summary>
public sealed class BowireAkkaProtocol : IBowireProtocol
{
    /// <summary>Service name shown in the Bowire sidebar.</summary>
    public const string TapServiceName = "Tap";

    /// <summary>Method name for the streaming subscription.</summary>
    public const string MonitorMethodName = "MonitorMessages";

    /// <summary>Method name for the per-actor throughput stream (#29).</summary>
    public const string ThroughputMethodName = "Throughput";

    private const int DefaultIntervalMs = 1000;
    private const int DefaultTop = 50;

    private ActorSystem? _system;

    /// <inheritdoc />
    public string Name => "Akka.NET";

    /// <inheritdoc />
    public string Id => "akka";

    /// <inheritdoc />
    public string IconSvg =>
        // Akka.NET community mark — concentric arcs simplified to a single
        // glyph. Not the official logo; keeps the plugin icon-self-
        // contained without a brand-asset bundle.
        """<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="3"/><path d="M3 12a9 9 0 0 1 9-9"/><path d="M12 21a9 9 0 0 1-9-9"/><path d="M21 12a9 9 0 0 1-9 9"/></svg>""";

    /// <inheritdoc />
    public void Initialize(IServiceProvider? serviceProvider)
    {
        // Embedded mode — pick up the host app's ActorSystem. Standalone
        // mode (no DI) leaves _system null and the methods below return
        // empty results until 1.1.0 wires the ClusterClient transport.
        _system = serviceProvider?.GetService(typeof(ActorSystem)) as ActorSystem;
    }

    /// <inheritdoc />
    public Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        // No live system — nothing to surface yet.
        if (_system is null)
        {
            return Task.FromResult(new List<BowireServiceInfo>());
        }

        var monitor = new BowireMethodInfo(
            Name: MonitorMethodName,
            FullName: $"{TapServiceName}/{MonitorMethodName}",
            ClientStreaming: false,
            ServerStreaming: true,
            InputType: new BowireMessageInfo("Empty", $"{TapServiceName}.Empty", []),
            OutputType: new BowireMessageInfo("TappedMessage", $"{TapServiceName}.TappedMessage", []),
            MethodType: "ServerStreaming");

        var throughput = new BowireMethodInfo(
            Name: ThroughputMethodName,
            FullName: $"{TapServiceName}/{ThroughputMethodName}",
            ClientStreaming: false,
            ServerStreaming: true,
            InputType: new BowireMessageInfo("ThroughputRequest", $"{TapServiceName}.ThroughputRequest",
            [
                new BowireFieldInfo("intervalMs", 1, "int32", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = $"How often a snapshot is sent, in milliseconds (100-60000, default {DefaultIntervalMs}).",
                },
                new BowireFieldInfo("top", 2, "int32", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = $"Only the busiest this many actors per snapshot; 0 for all (default {DefaultTop}).",
                },
            ]),
            OutputType: new BowireMessageInfo("ThroughputSnapshot", $"{TapServiceName}.ThroughputSnapshot", []),
            MethodType: "ServerStreaming")
        {
            Summary = "Messages per second per actor, counted at the tap.",
        };

        var service = new BowireServiceInfo(
            Name: TapServiceName,
            Package: Id,
            Methods: [monitor, throughput]);

        return Task.FromResult<List<BowireServiceInfo>>([service]);
    }

    /// <inheritdoc />
    public Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        // The Tap surface is observe-only; there's no unary call to make.
        return Task.FromResult(new InvokeResult(
            Response: """{ "info": "Akka tap is server-streaming only — invoke MonitorMessages via the streaming pane." }""",
            DurationMs: 0,
            Status: "stream-only",
            Metadata: new Dictionary<string, string>(StringComparer.Ordinal)));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> InvokeStreamAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_system is not ExtendedActorSystem ext)
        {
            // No live system — yield nothing. The UI shows an empty
            // stream which is correct: standalone mode without an
            // attached actor system has no taps to forward.
            yield break;
        }

        var extension = BowireAkkaExtensionProvider.Instance.Apply(ext);

        if (string.Equals(method, ThroughputMethodName, StringComparison.Ordinal))
        {
            var (interval, top) = ReadThroughputRequest(jsonMessages);
            await foreach (var snapshot in StreamThroughputAsync(extension, interval, top, ct).ConfigureAwait(false))
            {
                yield return JsonSerializer.Serialize(snapshot);
            }
            yield break;
        }

        var reader = extension.Subscribe(out var token);
        try
        {
            await foreach (var tap in reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return JsonSerializer.Serialize(tap);
            }
        }
        finally
        {
            extension.Unsubscribe(token);
        }
    }

    /// <summary>
    /// Interval and top-N from the request body; out-of-range or missing
    /// values fall back to the defaults rather than failing the stream.
    /// </summary>
    internal static (TimeSpan Interval, int Top) ReadThroughputRequest(List<string> jsonMessages)
    {
        var intervalMs = DefaultIntervalMs;
        var top = DefaultTop;
        var body = jsonMessages is { Count: > 0 } ? jsonMessages[0] : null;
        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                using var doc = JsonDocument.Parse(body);
                if (doc.RootElement.ValueKind == JsonValueKind.Object)
                {
                    if (doc.RootElement.TryGetProperty("intervalMs", out var i) && i.TryGetInt32(out var iv))
                        intervalMs = Math.Clamp(iv, 100, 60_000);
                    if (doc.RootElement.TryGetProperty("top", out var t) && t.TryGetInt32(out var tv) && tv >= 0)
                        top = tv;
                }
            }
            catch (JsonException)
            {
                // A body that is not JSON asks for nothing in particular.
            }
        }
        return (TimeSpan.FromMilliseconds(intervalMs), top);
    }

    /// <summary>
    /// One <see cref="ThroughputSnapshot"/> per interval until cancelled —
    /// also when nothing happened, so a quiet system still shows a live
    /// stream rather than one that looks stuck.
    /// </summary>
    internal static async IAsyncEnumerable<ThroughputSnapshot> StreamThroughputAsync(
        BowireAkkaExtension extension, TimeSpan interval, int top,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var counter = extension.StartCounting();
        try
        {
            var clock = System.Diagnostics.Stopwatch.StartNew();
            using var timer = new PeriodicTimer(interval);
            while (true)
            {
                try
                {
                    if (!await timer.WaitForNextTickAsync(ct).ConfigureAwait(false)) yield break;
                }
                catch (OperationCanceledException) { yield break; }

                // Measured, not assumed: a timer tick is late under load, and
                // per-second figures from the nominal interval would be off by
                // exactly that.
                var seconds = clock.Elapsed.TotalSeconds;
                clock.Restart();
                var actors = counter.Drain()
                    .Select(c => new ActorThroughput(c.Path, c.Messages, seconds > 0 ? c.Messages / seconds : 0, c.Total))
                    .OrderByDescending(a => a.Messages)
                    .ThenBy(a => a.Path, StringComparer.Ordinal);
                yield return new ThroughputSnapshot(
                    DateTime.UtcNow, seconds,
                    [.. top > 0 ? actors.Take(top) : actors]);
            }
        }
        finally
        {
            extension.StopCounting(counter);
        }
    }

    /// <inheritdoc />
    public Task<IBowireChannel?> OpenChannelAsync(
        string serverUrl, string service, string method,
        bool showInternalServices, Dictionary<string, string>? metadata = null,
        CancellationToken ct = default)
    {
        // No interactive duplex on the tap surface yet; future work could
        // expose a "send" channel that does Tell into selected actors (#32).
        return Task.FromResult<IBowireChannel?>(null);
    }
}
