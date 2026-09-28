// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Cluster.Tools.Client;
using Akka.Configuration;
using Kuestenlogik.Bowire.Models;

namespace Kuestenlogik.Bowire.Protocol.Akka.Remote;

/// <summary>
/// The CLI side of the remote tap (#27): the standalone bowire attaches to a
/// running cluster through ClusterClient and streams its taps, without being
/// hosted in it.
/// </summary>
/// <remarks>
/// <para>
/// The server URL is a contact point of the cluster:
/// <c>akka.tcp://Harbor@10.0.0.5:4053</c>. The host must run
/// <see cref="BowireRemoteTap.EnableBowireRemoteTap"/>; without it discovery
/// says so instead of showing an empty tab.
/// </para>
/// <para>
/// The CLI runs a small actor system of its own for remoting. The relay
/// sends back to it, so its address must be reachable from the cluster:
/// <c>?clientHost=</c> in the URL sets the host name it binds and
/// advertises (default <c>localhost</c>, which only reaches a cluster on
/// the same machine).
/// </para>
/// </remarks>
public sealed class BowireAkkaRemoteProtocol : IBowireProtocol
{
    private static readonly ConcurrentDictionary<string, Lazy<Connection>> Connections = new(StringComparer.Ordinal);
    private static readonly TimeSpan PingTimeout = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    public string Name => "Akka.NET (remote)";

    /// <inheritdoc />
    public string Id => "akka-remote";

    /// <inheritdoc />
    public string IconSvg => new BowireAkkaProtocol().IconSvg;

    /// <inheritdoc />
    public void Initialize(IServiceProvider? serviceProvider)
    {
        // Nothing to pick up: the cluster is reached by URL, not from DI.
    }

    /// <inheritdoc />
    public async Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        if (Contact.TryParse(serverUrl) is not { } contact) return [];
        var connection = Connect(contact);
        try
        {
            await connection.Client.Ask<RemoteTapPong>(
                new ClusterClient.Send(BowireRemoteTap.RelayPath, new RemoteTapPing(contact.SystemName)),
                PingTimeout, ct).ConfigureAwait(false);
        }
        catch (AskTimeoutException)
        {
            throw new InvalidOperationException(
                $"No Bowire remote tap answered at {contact.Address} within {PingTimeout.TotalSeconds:0} s. " +
                "The host has to call EnableBowireRemoteTap(), run Akka.Cluster, and be able to reach " +
                $"this CLI at {connection.System.Name} (see ?clientHost=).");
        }

        var monitor = new BowireMethodInfo(
            Name: BowireAkkaProtocol.MonitorMethodName,
            FullName: $"{BowireAkkaProtocol.TapServiceName}/{BowireAkkaProtocol.MonitorMethodName}",
            ClientStreaming: false,
            ServerStreaming: true,
            InputType: new BowireMessageInfo("MonitorRequest", $"{BowireAkkaProtocol.TapServiceName}.MonitorRequest",
            [
                new BowireFieldInfo("paths", 1, "string", "repeated", IsMap: false, IsRepeated: true, MessageType: null, EnumValues: null)
                { Description = "Only messages to actors matching one of these; * for any characters (e.g. /user/dock-*)." },
                new BowireFieldInfo("messageTypes", 2, "string", "repeated", IsMap: false, IsRepeated: true, MessageType: null, EnumValues: null)
                { Description = "Only messages of a type matching one of these; full or simple type name, * for any characters." },
                new BowireFieldInfo("includeDeadLetters", 3, "bool", "optional", IsMap: false, IsRepeated: false, MessageType: null, EnumValues: null)
                { Description = "Whether dead letters come through (default true)." },
                new BowireFieldInfo("payloadFormat", 4, "enum", "optional", IsMap: false, IsRepeated: false, MessageType: null,
                    EnumValues: [.. Enum.GetValues<PayloadFormat>().Select(f => new BowireEnumValue(BowireAkkaProtocol.PayloadFormatName(f), (int)f))])
                { Description = "Also send each message as a JSON object, rendered on the host: none, auto, properties, fields, akka." },
            ]),
            OutputType: new BowireMessageInfo("TappedMessage", $"{BowireAkkaProtocol.TapServiceName}.TappedMessage", []),
            MethodType: "ServerStreaming")
        {
            Summary = $"Every tapped message in {contact.SystemName}, relayed over ClusterClient.",
        };

        return [new BowireServiceInfo(Name: BowireAkkaProtocol.TapServiceName, Package: Id, Methods: [monitor])];
    }

    /// <inheritdoc />
    public Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default) =>
        Task.FromResult(new InvokeResult(
            Response: """{ "info": "The remote tap is server-streaming only — stream MonitorMessages." }""",
            DurationMs: 0,
            Status: "stream-only",
            Metadata: new Dictionary<string, string>(StringComparer.Ordinal)));

    /// <inheritdoc />
    public async IAsyncEnumerable<string> InvokeStreamAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (Contact.TryParse(serverUrl) is not { } contact) yield break;
        var connection = Connect(contact);

        var filter = BowireAkkaProtocol.ReadMonitorRequest(jsonMessages);
        var subscribe = new RemoteTapSubscribe(
            SubscriptionId: Guid.NewGuid().ToString("N"),
            Paths: filter?.Paths ?? [],
            MessageTypes: filter?.MessageTypes ?? [],
            IncludeDeadLetters: filter?.IncludeDeadLetters ?? true,
            PayloadFormat: (int)BowireAkkaProtocol.ReadPayloadFormat(jsonMessages));

        var inbox = Channel.CreateBounded<RemoteTapped>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });
        var receiver = connection.System.ActorOf(Props.Create(() => new Receiver(inbox.Writer)));
        var send = new ClusterClient.Send(BowireRemoteTap.RelayPath, subscribe);
        connection.Client.Tell(send, receiver);

        using var renew = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _ = RenewAsync(connection.Client, send, receiver, renew.Token);
        try
        {
            await foreach (var m in inbox.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return JsonSerializer.Serialize(new TappedMessage(
                    m.Recipient, m.Sender, m.MessageType, m.Payload, m.Timestamp, m.IsDeadLetter, Parse(m.PayloadJson)));
            }
        }
        finally
        {
            await renew.CancelAsync().ConfigureAwait(false);
            connection.Client.Tell(new ClusterClient.Send(BowireRemoteTap.RelayPath, new RemoteTapUnsubscribe(subscribe.SubscriptionId)));
            connection.System.Stop(receiver);
        }
    }

    /// <inheritdoc />
    public Task<IBowireChannel?> OpenChannelAsync(
        string serverUrl, string service, string method,
        bool showInternalServices, Dictionary<string, string>? metadata = null,
        CancellationToken ct = default) =>
        // Read-only: the remote tap never sends into the cluster.
        Task.FromResult<IBowireChannel?>(null);

    private static async Task RenewAsync(IActorRef client, ClusterClient.Send subscribe, IActorRef receiver, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(BowireRemoteTap.KeepAliveInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
                client.Tell(subscribe, receiver);
        }
        catch (OperationCanceledException) { /* the stream ended */ }
    }

    private static JsonElement? Parse(string? json)
    {
        if (string.IsNullOrEmpty(json)) return null;
        try
        {
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static Connection Connect(Contact contact) =>
        Connections.GetOrAdd(contact.Key, _ => new Lazy<Connection>(() => Connection.Open(contact))).Value;

    /// <summary>Close every client actor system — for tests and orderly shutdown.</summary>
    internal static async Task ShutdownAllAsync()
    {
        foreach (var lazy in Connections.Values.Where(l => l.IsValueCreated))
            await lazy.Value.System.Terminate().ConfigureAwait(false);
        Connections.Clear();
    }

    /// <summary>A cluster contact point from a server URL.</summary>
    internal sealed record Contact(string Address, string SystemName, string ClientHost)
    {
        public string Key => $"{Address}|{ClientHost}";

        internal static Contact? TryParse(string? url)
        {
            if (string.IsNullOrWhiteSpace(url)) return null;
            var query = url.IndexOf('?', StringComparison.Ordinal);
            var address = (query < 0 ? url : url[..query]).TrimEnd('/');
            var clientHost = "localhost";
            if (query >= 0)
            {
                foreach (var pair in url[(query + 1)..].Split('&', StringSplitOptions.RemoveEmptyEntries))
                {
                    var kv = pair.Split('=', 2);
                    if (kv.Length == 2 && kv[0] == "clientHost" && !string.IsNullOrWhiteSpace(kv[1])) clientHost = Uri.UnescapeDataString(kv[1]);
                }
            }
            if (!address.StartsWith("akka.tcp://", StringComparison.Ordinal) && !address.StartsWith("akka://", StringComparison.Ordinal)) return null;
            try
            {
                var parsed = global::Akka.Actor.Address.Parse(address.Replace("akka://", "akka.tcp://", StringComparison.Ordinal));
                if (parsed.Host is null || parsed.Port is null) return null;
                return new Contact(parsed.ToString(), parsed.System, clientHost);
            }
            catch (Exception ex) when (ex is UriFormatException or FormatException or ArgumentException)
            {
                return null;
            }
        }
    }

    private sealed record Connection(ActorSystem System, IActorRef Client)
    {
        public static Connection Open(Contact contact)
        {
            var config = ConfigurationFactory.ParseString($$"""
                akka.actor.provider = remote
                akka.remote.dot-netty.tcp {
                  hostname = "{{contact.ClientHost}}"
                  port = 0
                }
                akka.loglevel = WARNING
                """).WithFallback(ClusterClientReceptionist.DefaultConfig());
            // Lives as long as the CLI: one per contact point, reused by every
            // stream, terminated by ShutdownAllAsync or with the process.
#pragma warning disable CA2000
            var system = ActorSystem.Create("bowire-cli", config);
#pragma warning restore CA2000
            var settings = ClusterClientSettings.Create(system)
                .WithInitialContacts(ImmutableHashSet.Create(ActorPath.Parse($"{contact.Address}/system/receptionist")));
            var client = system.ActorOf(ClusterClient.Props(settings), "bowire-cluster-client");
            return new Connection(system, client);
        }
    }

#pragma warning disable CA1812 // instantiated by Akka via Props.Create
    private sealed class Receiver : ReceiveActor
    {
        private readonly ChannelWriter<RemoteTapped> _writer;

        public Receiver(ChannelWriter<RemoteTapped> writer)
        {
            _writer = writer;
            Receive<RemoteTapped>(m => _writer.TryWrite(m));
            Receive<RemoteTapAlive>(_ => { /* the relay is there; nothing to show */ });
        }

        protected override void PostStop() => _writer.TryComplete();
    }
#pragma warning restore CA1812
}
