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

    /// <summary>Method name for the mailbox view (#28).</summary>
    public const string MailboxesMethodName = "Mailboxes";

    private const int DefaultHead = 5;
    private const int MaxHead = 100;

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
            InputType: new BowireMessageInfo("MonitorRequest", $"{TapServiceName}.MonitorRequest",
            [
                new BowireFieldInfo("paths", 1, "string", "repeated", IsMap: false, IsRepeated: true, MessageType: null, EnumValues: null)
                {
                    Description = "Only messages to actors matching one of these; * for any characters, full path or from /user on (e.g. /user/dock-*).",
                },
                new BowireFieldInfo("messageTypes", 2, "string", "repeated", IsMap: false, IsRepeated: true, MessageType: null, EnumValues: null)
                {
                    Description = "Only messages of a type matching one of these; full or simple type name, * for any characters (e.g. PortCall*).",
                },
                new BowireFieldInfo("includeDeadLetters", 3, "bool", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = "Whether dead letters come through (default true).",
                },
                new BowireFieldInfo("typedPayload", 4, "bool", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = "Also send each message as structured JSON (PayloadJson), serialized the way the actor system is configured to (default false).",
                },
            ]),
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

        var mailboxes = new BowireMethodInfo(
            Name: MailboxesMethodName,
            FullName: $"{TapServiceName}/{MailboxesMethodName}",
            ClientStreaming: false,
            ServerStreaming: false,
            InputType: new BowireMessageInfo("MailboxesRequest", $"{TapServiceName}.MailboxesRequest",
            [
                new BowireFieldInfo("path", 1, "string", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = "Only actors whose path starts with this, e.g. akka://Harbor/user/dock.",
                },
                new BowireFieldInfo("head", 2, "int32", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = $"Queued messages to show per mailbox, oldest first (0-{MaxHead}, default {DefaultHead}).",
                },
                new BowireFieldInfo("nonEmptyOnly", 3, "bool", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                {
                    Description = "Leave out mailboxes with nothing queued (default false).",
                },
            ]),
            OutputType: new BowireMessageInfo("MailboxSnapshot", $"{TapServiceName}.MailboxSnapshot", []),
            MethodType: "Unary")
        {
            Summary = "Depth and the next queued messages of each tap mailbox, without taking any off.",
        };

        var service = new BowireServiceInfo(
            Name: TapServiceName,
            Package: Id,
            Methods: [monitor, throughput, mailboxes]);

        return Task.FromResult<List<BowireServiceInfo>>([service]);
    }

    /// <inheritdoc />
    public Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        if (string.Equals(method, MailboxesMethodName, StringComparison.Ordinal) && _system is ExtendedActorSystem ext)
        {
            var (path, head, nonEmptyOnly) = ReadMailboxesRequest(jsonMessages);
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var snapshot = BowireAkkaExtensionProvider.Instance.Apply(ext).SnapshotMailboxes(path, head, nonEmptyOnly);
            return Task.FromResult(new InvokeResult(
                Response: JsonSerializer.Serialize(snapshot),
                DurationMs: watch.ElapsedMilliseconds,
                Status: "OK",
                Metadata: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["mailboxes"] = snapshot.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
                }));
        }

        // The rest of the Tap surface is streaming; there's no unary call to make.
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

        var reader = extension.Subscribe(ReadMonitorRequest(jsonMessages), ReadTypedPayload(jsonMessages), out var token);
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
    /// The filter a MonitorMessages request asks for, or null for everything
    /// (#31). A body that is not a JSON object asks for everything.
    /// </summary>
    internal static TapFilter? ReadMonitorRequest(List<string> jsonMessages)
    {
        var body = jsonMessages is { Count: > 0 } ? jsonMessages[0] : null;
        if (string.IsNullOrWhiteSpace(body)) return null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return null;
            var includeDead = !(root.TryGetProperty("includeDeadLetters", out var d) && d.ValueKind == JsonValueKind.False);
            var filter = new TapFilter(Strings(root, "paths"), Strings(root, "messageTypes"), includeDead);
            return filter.IsEmpty ? null : filter;
        }
        catch (JsonException)
        {
            return null;
        }

        // An array of strings, or a single string for one pattern.
        static List<string> Strings(JsonElement root, string name)
        {
            if (!root.TryGetProperty(name, out var v)) return [];
            return v.ValueKind switch
            {
                JsonValueKind.String => [v.GetString()!],
                JsonValueKind.Array => [.. v.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!)],
                _ => [],
            };
        }
    }

    /// <summary>Whether a MonitorMessages request asks for structured payloads (#30).</summary>
    internal static bool ReadTypedPayload(List<string> jsonMessages)
    {
        var body = jsonMessages is { Count: > 0 } ? jsonMessages[0] : null;
        if (string.IsNullOrWhiteSpace(body)) return false;
        try
        {
            using var doc = JsonDocument.Parse(body);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                && doc.RootElement.TryGetProperty("typedPayload", out var t)
                && t.ValueKind == JsonValueKind.True;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>Path prefix, head size and filter from the request body; bad values fall back to the defaults.</summary>
    internal static (string? Path, int Head, bool NonEmptyOnly) ReadMailboxesRequest(List<string> jsonMessages)
    {
        string? path = null;
        var head = DefaultHead;
        var nonEmptyOnly = false;
        var body = jsonMessages is { Count: > 0 } ? jsonMessages[0] : null;
        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;
                if (root.ValueKind == JsonValueKind.Object)
                {
                    if (root.TryGetProperty("path", out var p) && p.ValueKind == JsonValueKind.String && !string.IsNullOrEmpty(p.GetString()))
                        path = p.GetString();
                    if (root.TryGetProperty("head", out var h) && h.TryGetInt32(out var hv))
                        head = Math.Clamp(hv, 0, MaxHead);
                    if (root.TryGetProperty("nonEmptyOnly", out var n) && n.ValueKind is JsonValueKind.True or JsonValueKind.False)
                        nonEmptyOnly = n.GetBoolean();
                }
            }
            catch (JsonException)
            {
                // A body that is not JSON asks for nothing in particular.
            }
        }
        return (path, head, nonEmptyOnly);
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
