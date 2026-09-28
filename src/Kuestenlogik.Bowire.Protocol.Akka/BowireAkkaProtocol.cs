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
/// Embedded mode: the plugin grabs the host app's <see cref="ActorSystem"/>
/// from DI and reads from <see cref="BowireAkkaExtension"/>. The standalone
/// CLI attaches to a cluster through the separate
/// <c>Kuestenlogik.Bowire.Protocol.Akka.Remote</c> package instead (#27).
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

    /// <summary>Method name for sending into the actor system (#32) — listed only when the host allows it.</summary>
    public const string TellMethodName = "Tell";

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
        // Embedded mode — pick up the host app's ActorSystem. Without one
        // (the standalone CLI) the methods below return nothing; attaching
        // to a cluster is the Remote package's job (#27).
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
                new BowireFieldInfo("payloadFormat", 4, "enum", "optional", IsMap: false, IsRepeated: false, MessageType: null,
                    EnumValues: [.. Enum.GetValues<PayloadFormat>().Select(f => new BowireEnumValue(PayloadFormatName(f), (int)f))])
                {
                    Description = "Also send each message as a JSON object (PayloadJson): none (default), auto (most readable), properties (public properties), fields (instance fields, private too), akka (what the configured serializer puts on the wire).",
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

        List<BowireMethodInfo> methods = [monitor, throughput, mailboxes];
        if (_system is ExtendedActorSystem system
            && BowireAkkaExtensionProvider.Instance.Apply(system).TellPolicy is { } policy)
        {
            methods.Add(TellMethod(policy));
        }

        var service = new BowireServiceInfo(
            Name: TapServiceName,
            Package: Id,
            Methods: methods);

        return Task.FromResult<List<BowireServiceInfo>>([service]);
    }

    /// <inheritdoc />
    public Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        if (string.Equals(method, TellMethodName, StringComparison.Ordinal))
            return TellOnceAsync(jsonMessages, ct);

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

        var reader = extension.Subscribe(ReadMonitorRequest(jsonMessages), ReadPayloadFormat(jsonMessages), out var token);
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

    /// <summary>
    /// One tell from the invoke pane (#32); with <c>replyTimeoutMs</c> it
    /// waits for the first reply, as an Ask.
    /// </summary>
    private async Task<InvokeResult> TellOnceAsync(List<string> jsonMessages, CancellationToken ct)
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();
        var body = jsonMessages is { Count: > 0 } ? jsonMessages[0] : "{}";
        if (_system is not ExtendedActorSystem ext)
            return Refused("No actor system is attached.");
        var extension = BowireAkkaExtensionProvider.Instance.Apply(ext);
        var (target, refusal) = TellGateway.Resolve(ext, extension.TellPolicy, body);
        if (target is null) return Refused(refusal!);

        var timeout = 0;
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("replyTimeoutMs", out var t) && t.TryGetInt32(out var tv))
                timeout = Math.Clamp(tv, 0, 30_000);
        }
        catch (JsonException) { /* Resolve accepted it; nothing more to read */ }

        extension.AuditTell(target);
        if (timeout == 0)
        {
            target.Selection.Tell(target.Message, ActorRefs.NoSender);
            return Result(new { sent = true, path = target.Path }, "OK");
        }

        try
        {
            var reply = await target.Selection.Ask<object>(target.Message, TimeSpan.FromMilliseconds(timeout), ct).ConfigureAwait(false);
            return Result(new
            {
                sent = true,
                path = target.Path,
                reply = new TellReply(target.Path, reply.GetType().FullName ?? "<null>", reply.ToString() ?? "",
                    PayloadRenderer.Render(ext, reply, PayloadFormat.Auto), DateTime.UtcNow),
            }, "OK");
        }
        catch (AskTimeoutException)
        {
            return Result(new { sent = true, path = target.Path, reply = (object?)null, note = $"No reply within {timeout} ms." }, "OK");
        }

        InvokeResult Refused(string reason) => Result(new { refused = reason }, "refused");

        InvokeResult Result(object response, string status) => new(
            Response: JsonSerializer.Serialize(response),
            DurationMs: watch.ElapsedMilliseconds,
            Status: status,
            Metadata: new Dictionary<string, string>(StringComparer.Ordinal));
    }

    /// <summary>
    /// The Tell method as the host's policy allows it: the message types to
    /// choose from, the path patterns in the description.
    /// </summary>
    private static BowireMethodInfo TellMethod(TellPolicy policy) => new(
        Name: TellMethodName,
        FullName: $"{TapServiceName}/{TellMethodName}",
        ClientStreaming: true,
        ServerStreaming: true,
        InputType: new BowireMessageInfo("TellRequest", $"{TapServiceName}.TellRequest",
        [
            new BowireFieldInfo("path", 1, "string", "required", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
            {
                Required = true,
                Description = $"A concrete actor path, e.g. /user/dock-1. Allowed: {string.Join(", ", policy.Paths)}.",
            },
            new BowireFieldInfo("messageType", 2, "enum", "required", IsMap: false, IsRepeated: false, MessageType: null,
                EnumValues: [.. policy.MessageTypes.Keys.Order(StringComparer.Ordinal).Select((name, i) => new BowireEnumValue(name, i))])
            {
                Required = true,
                Description = "The message type; the host lists which ones may be sent.",
            },
            new BowireFieldInfo("message", 3, "object", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
            {
                Description = "The message as JSON, deserialized into the chosen type.",
            },
            new BowireFieldInfo("replyTimeoutMs", 4, "int32", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
            {
                Description = "Invoke pane only: wait this long for a reply (0-30000, default 0 = don't wait).",
            },
        ]),
        OutputType: new BowireMessageInfo("TellReply", $"{TapServiceName}.TellReply", []),
        MethodType: "Duplex")
    {
        Summary = "Send a message to an actor; replies come back here. Allowed actors and types are set by the host.",
    };

    /// <summary>The name a payload format goes by in a request.</summary>
    internal static string PayloadFormatName(PayloadFormat format) => format switch
    {
        PayloadFormat.Auto => "auto",
        PayloadFormat.Properties => "properties",
        PayloadFormat.Fields => "fields",
        PayloadFormat.Akka => "akka",
        _ => "none",
    };

    /// <summary>
    /// The structured payload a MonitorMessages request asks for (#30), by
    /// name or number; anything not understood means none.
    /// </summary>
    internal static PayloadFormat ReadPayloadFormat(List<string> jsonMessages)
    {
        var body = jsonMessages is { Count: > 0 } ? jsonMessages[0] : null;
        if (string.IsNullOrWhiteSpace(body)) return PayloadFormat.None;
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Object
                || !doc.RootElement.TryGetProperty("payloadFormat", out var f)) return PayloadFormat.None;
            if (f.ValueKind == JsonValueKind.String
                && Enum.TryParse<PayloadFormat>(f.GetString(), ignoreCase: true, out var named)
                && Enum.IsDefined(named)
                && !int.TryParse(f.GetString(), out _))
                return named;
            if (f.ValueKind == JsonValueKind.Number && f.TryGetInt32(out var n) && Enum.IsDefined((PayloadFormat)n))
                return (PayloadFormat)n;
            return PayloadFormat.None;
        }
        catch (JsonException)
        {
            return PayloadFormat.None;
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
        // Only Tell is a channel, and only where the host allows it (#32).
        if (!string.Equals(method, TellMethodName, StringComparison.Ordinal)
            || _system is not ExtendedActorSystem ext
            || BowireAkkaExtensionProvider.Instance.Apply(ext).TellPolicy is null)
        {
            return Task.FromResult<IBowireChannel?>(null);
        }
        return Task.FromResult<IBowireChannel?>(new BowireTellChannel(ext, BowireAkkaExtensionProvider.Instance.Apply(ext)));
    }
}
