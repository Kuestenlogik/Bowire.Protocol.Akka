// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Threading.Channels;
using Akka.Actor;
using Akka.Cluster.Tools.Client;
using Akka.Event;

namespace Kuestenlogik.Bowire.Protocol.Akka.Remote;

/// <summary>
/// The host side of the remote tap (#27): a relay the standalone bowire CLI
/// reaches through ClusterClient.
/// </summary>
/// <remarks>
/// <para>
/// Off unless the host calls <see cref="EnableBowireRemoteTap"/>. It is
/// read-only — it relays taps, it never delivers a message into the system —
/// but what it relays is message content, over Akka remoting, which is not
/// authenticated unless the host configures TLS. Enable it where the network
/// is trusted, or with remoting secured.
/// </para>
/// <para>
/// ClusterClient replies travel through a response tunnel that closes after
/// a quiet spell (30 s by default). So the client renews its subscription
/// every <see cref="KeepAliveInterval"/> — the relay takes each renewal's
/// sender as the new way back — and the relay sends a heartbeat when it has
/// had nothing to send. A subscription not renewed for <see cref="Lease"/>
/// ends: a CLI that went away does not keep the relay tapping.
/// </para>
/// </remarks>
public static class BowireRemoteTap
{
    /// <summary>Path of the relay under <c>/user</c>, as ClusterClient addresses it.</summary>
    public const string RelayName = "bowire-tap-relay";

    /// <summary>The path ClusterClient sends to.</summary>
    public const string RelayPath = "/user/" + RelayName;

    /// <summary>
    /// How often the client renews, and — by default — how long the relay
    /// stays quiet before it sends a heartbeat
    /// (<c>bowire.akka.remote-tap.heartbeat</c>).
    /// </summary>
    public static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a subscription lives without renewal, by default
    /// (<c>bowire.akka.remote-tap.lease</c>). Keep it well above
    /// <see cref="KeepAliveInterval"/>: a renewal lost on the way should not
    /// end a subscription.
    /// </summary>
    public static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    private static TimeSpan Setting(ActorSystem system, string key, TimeSpan fallback)
    {
        var config = system.Settings.Config;
        var path = "bowire.akka.remote-tap." + key;
        return config.HasPath(path) ? config.GetTimeSpan(path, fallback, allowInfinite: false) : fallback;
    }

    /// <summary>
    /// Start the relay and register it with the ClusterClient receptionist.
    /// The actor system has to run Akka.Cluster. Calling it twice returns the
    /// relay that is already there.
    /// </summary>
    public static IActorRef EnableBowireRemoteTap(this ActorSystem system)
    {
        ArgumentNullException.ThrowIfNull(system);
        if (system is not ExtendedActorSystem ext)
            throw new ArgumentException("The remote tap needs an ExtendedActorSystem.", nameof(system));

        lock (Relays)
        {
            if (Relays.TryGetValue(system, out var existing)) return existing;
            var relay = system.ActorOf(Props.Create(() => new RelayActor(ext)), RelayName);
            ClusterClientReceptionist.Get(system).RegisterService(relay);
            Relays.Add(system, relay);
            system.Log.Warning(
                "Bowire: remote tap enabled at {0} — message contents are relayed to ClusterClients over remoting.",
                relay.Path);
            return relay;
        }
    }

    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<ActorSystem, IActorRef> Relays = [];

    internal sealed class RelayActor : ReceiveActor
    {
        private readonly Dictionary<string, IActorRef> _subscriptions = new(StringComparer.Ordinal);
        private readonly TimeSpan _lease;
        private readonly TimeSpan _heartbeat;

        public RelayActor(ExtendedActorSystem system)
        {
            _lease = Setting(system, "lease", Lease);
            _heartbeat = Setting(system, "heartbeat", KeepAliveInterval);
            Receive<RemoteTapPing>(_ => Sender.Tell(new RemoteTapPong(system.Name)));
            Receive<RemoteTapSubscribe>(s =>
            {
                if (_subscriptions.TryGetValue(s.SubscriptionId, out var existing))
                {
                    existing.Forward(s);
                    return;
                }
                var child = Context.ActorOf(Props.Create(() => new SubscriptionActor(system, s, Sender, _lease, _heartbeat)));
                Context.Watch(child);
                _subscriptions[s.SubscriptionId] = child;
            });
            Receive<RemoteTapUnsubscribe>(u =>
            {
                if (_subscriptions.Remove(u.SubscriptionId, out var child)) Context.Stop(child);
            });
            Receive<Terminated>(t =>
            {
                foreach (var (id, child) in _subscriptions.Where(kv => kv.Value.Equals(t.ActorRef)).ToList())
                    _subscriptions.Remove(id);
            });
        }
    }

    /// <summary>One CLI's subscription: pumps the tap to wherever it was last renewed from.</summary>
    // An actor's end is PostStop, which cancels and disposes the pump.
#pragma warning disable CA1001
    internal sealed class SubscriptionActor : ReceiveActor, IWithTimers
    {
        private sealed record Tapped(TappedMessage Message);
        private sealed record Tick;

        private readonly BowireAkkaExtension _extension;
        private readonly string _id;
        private IActorRef _client;
        private object? _token;
        private DateTime _lastRenewal = DateTime.UtcNow;
        private DateTime _lastSent = DateTime.UtcNow;
        private readonly CancellationTokenSource _pump = new();
        private readonly TimeSpan _lease;
        private readonly TimeSpan _heartbeat;

        public ITimerScheduler Timers { get; set; } = null!;

        public SubscriptionActor(ExtendedActorSystem system, RemoteTapSubscribe request, IActorRef client, TimeSpan lease, TimeSpan heartbeat)
        {
            _lease = lease;
            _heartbeat = heartbeat;
            _extension = BowireAkkaExtensionProvider.Instance.Apply(system);
            _id = request.SubscriptionId;
            _client = client;

            var filter = new TapFilter(request.Paths, request.MessageTypes, request.IncludeDeadLetters);
            var format = Enum.IsDefined((PayloadFormat)request.PayloadFormat) ? (PayloadFormat)request.PayloadFormat : PayloadFormat.None;
            var reader = _extension.Subscribe(filter.IsEmpty ? null : filter, format, out var token);
            _token = token;
            var self = Self;
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var m in reader.ReadAllAsync(_pump.Token).ConfigureAwait(false))
                        self.Tell(new Tapped(m));
                }
                catch (OperationCanceledException) { /* stopped */ }
                catch (ChannelClosedException) { /* unsubscribed */ }
            });

            Receive<Tapped>(t =>
            {
                var m = t.Message;
                _client.Tell(new RemoteTapped(_id, m.Recipient, m.Sender, m.MessageType, m.Payload, m.Timestamp,
                    m.IsDeadLetter, m.PayloadJson?.GetRawText()));
                _lastSent = DateTime.UtcNow;
            });
            Receive<RemoteTapSubscribe>(_ =>
            {
                // A renewal: its sender is the current way back to the client.
                _client = Sender;
                _lastRenewal = DateTime.UtcNow;
            });
            Receive<Tick>(_ =>
            {
                if (DateTime.UtcNow - _lastRenewal > _lease)
                {
                    Context.Stop(Self);
                    return;
                }
                if (DateTime.UtcNow - _lastSent >= _heartbeat)
                {
                    _client.Tell(new RemoteTapAlive(_id));
                    _lastSent = DateTime.UtcNow;
                }
            });
        }

        protected override void PreStart()
        {
            _client.Tell(new RemoteTapAlive(_id));
            var tick = TimeSpan.FromTicks(Math.Clamp(Math.Min(_lease.Ticks, _heartbeat.Ticks) / 4, TimeSpan.FromMilliseconds(100).Ticks, TimeSpan.FromSeconds(1).Ticks));
            Timers.StartPeriodicTimer("tick", new Tick(), tick);
        }

        protected override void PostStop()
        {
            _pump.Cancel();
            _pump.Dispose();
            if (_token is not null) _extension.Unsubscribe(_token);
            _token = null;
        }
    }
#pragma warning restore CA1001
}
