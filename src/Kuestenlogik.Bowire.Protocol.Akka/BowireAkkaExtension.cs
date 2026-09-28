// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// Akka.NET extension that owns the per-actor-system tap state for the
/// Bowire workbench: the list of active subscriber channels that
/// <see cref="BowireTapMailbox"/> fans every tapped enqueue out to — each
/// a bounded, drop-oldest channel so a slow reader can't stall the actor
/// system — plus a dead-letter bridge that republishes the system's
/// undeliverable messages through the same fan-out.
/// <para>
/// Subscribers (the Bowire UI's <see cref="BowireAkkaProtocol.InvokeStreamAsync"/>
/// implementation) call <see cref="Subscribe"/> to get a fresh
/// <see cref="ChannelReader{T}"/> that receives every tap from now on.
/// Multiple subscribers each get their own reader — no fan-out coupling.
/// </para>
/// <para>
/// The extension also subscribes to the actor system's
/// <see cref="EventStream"/> for <see cref="DeadLetter"/> events and
/// republishes them through the same channel with
/// <see cref="TappedMessage.IsDeadLetter"/> set, so undeliverable messages
/// surface in the Bowire stream without any per-actor opt-in.
/// </para>
/// </summary>
/// <remarks>
/// The tap mailbox queries this extension on every enqueue; if no
/// subscribers are active the call returns immediately, so the steady-
/// state cost when Bowire isn't watching is one volatile field read per
/// message — effectively negligible.
/// </remarks>
public sealed class BowireAkkaExtension : IExtension
{
    private readonly object _lock = new();
    private readonly List<Channel<TappedMessage>> _subscribers = [];
    private readonly List<ThroughputCounter> _counters = [];
    // Created on the first Subscribe, not here — see EnsureDeadLetterBridge.
    // Its own lock: creating an actor while holding _lock, which every tapped
    // enqueue takes, is one scheduling decision away from a deadlock in the
    // global-default mode, where the new actor's mailbox is a tap as well.
    private readonly object _bridgeLock = new();
    private IActorRef? _deadLetterListener;
    private readonly string _deadLetterPath;

    /// <summary>The actor system this extension instance belongs to.</summary>
    public ExtendedActorSystem System { get; }

    internal BowireAkkaExtension(ExtendedActorSystem system)
    {
        System = system;
        _deadLetterPath = system.DeadLetters.Path.ToString();

        // The dead-letter bridge is NOT spawned here (#33). With the tap as
        // the *global* default mailbox this constructor runs while the root
        // guardian's own mailbox is being built — before the system can host
        // an actor — and the spawn threw. A catch-all hid it, and dead-letter
        // capture was silently off in exactly that mode. The bridge is only
        // needed once somebody is watching (Publish drops everything with no
        // subscriber), and by the time somebody subscribes the system is up.

        // Tear down the subscription when the system shuts down so we
        // don't leak the EventStream registration. IExtension has no
        // dispose hook, so RegisterOnTermination is the standard knob.
        system.RegisterOnTermination(() =>
        {
            try
            {
                if (_deadLetterListener is { } listener)
                {
                    System.EventStream.Unsubscribe(listener, typeof(DeadLetter));
                }
            }
            catch
            {
                // Best-effort cleanup during shutdown; swallow.
            }
        });
    }

    /// <summary>
    /// True when at least one Bowire subscriber is listening — to the
    /// messages or to the throughput. Read by the tap mailbox on every
    /// enqueue to short-circuit the marshalling path when nobody's watching.
    /// </summary>
    public bool HasSubscribers
    {
        get
        {
            lock (_lock)
            {
                return _subscribers.Count > 0 || _counters.Count > 0;
            }
        }
    }

    /// <summary>
    /// Start counting messages per actor path (#29). The counter sees every
    /// tapped enqueue and every dead letter from now on; pass it to
    /// <see cref="StopCounting"/> when done.
    /// </summary>
    public ThroughputCounter StartCounting()
    {
        var counter = new ThroughputCounter();
        lock (_lock)
        {
            _counters.Add(counter);
        }
        // Dead letters count too, under the dead-letter path — the bridge is
        // what reports them, so it has to exist for a counter as for a reader.
        EnsureDeadLetterBridge();
        return counter;
    }

    /// <summary>Tear-down counterpart to <see cref="StartCounting"/>.</summary>
    public void StopCounting(ThroughputCounter counter)
    {
        lock (_lock)
        {
            _counters.Remove(counter);
        }
    }

    /// <summary>
    /// Open a fresh reader. Each subscriber writes into its own bounded
    /// channel (drop-oldest on overflow) so a slow subscriber can't stall
    /// the actor system. Caller disposes by passing the returned token to
    /// <see cref="Unsubscribe"/> once the stream ends.
    /// </summary>
    public ChannelReader<TappedMessage> Subscribe(out object token)
    {
        var ch = Channel.CreateBounded<TappedMessage>(new BoundedChannelOptions(capacity: 1024)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
        lock (_lock)
        {
            _subscribers.Add(ch);
        }
        EnsureDeadLetterBridge();
        token = ch;
        return ch.Reader;
    }

    /// <summary>
    /// Spawn the dead-letter listener and subscribe it to the
    /// <see cref="EventStream"/>, once, on the first subscriber (#33).
    /// </summary>
    /// <remarks>
    /// A failure is logged through the actor system's own log rather than
    /// swallowed: the old catch-all is how dead-letter capture came to be off
    /// without anybody being told. It is retried on the next subscribe, so a
    /// transient failure does not disable capture for the life of the system.
    /// </remarks>
    private void EnsureDeadLetterBridge()
    {
        lock (_bridgeLock)
        {
            if (_deadLetterListener is not null) return;
            try
            {
                var listener = System.SystemActorOf(
                    Props.Create(() => new DeadLetterListener(this)),
                    "bowire-deadletter-listener");
                System.EventStream.Subscribe(listener, typeof(DeadLetter));
                _deadLetterListener = listener;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                System.Log.Warning(
                    "Bowire: dead-letter capture is not available ({0}: {1}); live mailbox taps are unaffected.",
                    ex.GetType().Name, ex.Message);
            }
        }
    }

    /// <summary>Tear-down counterpart to <see cref="Subscribe"/>.</summary>
    public void Unsubscribe(object token)
    {
        if (token is not Channel<TappedMessage> ch) return;
        lock (_lock)
        {
            _subscribers.Remove(ch);
        }
        ch.Writer.TryComplete();
    }

    /// <summary>
    /// Called by <see cref="BowireTapMessageQueue.Enqueue"/> for every
    /// tapped message. Fan-out: written to each subscriber's channel
    /// without awaiting (drop-oldest takes care of slow consumers).
    /// </summary>
    internal void Publish(TappedMessage msg)
    {
        // Snapshot under lock; write outside to keep the hot path short.
        Channel<TappedMessage>[] snapshot;
        ThroughputCounter[] counters;
        lock (_lock)
        {
            if (_subscribers.Count == 0 && _counters.Count == 0) return;
            snapshot = _subscribers.Count == 0 ? [] : _subscribers.ToArray();
            counters = _counters.Count == 0 ? [] : _counters.ToArray();
        }
        // Counted before the channels, which may drop: the throughput is what
        // the tap saw, not what a reader managed to keep up with.
        foreach (var counter in counters)
        {
            counter.Record(msg.Recipient);
        }
        foreach (var ch in snapshot)
        {
            ch.Writer.TryWrite(msg);
        }
    }

    /// <summary>
    /// Convert a <see cref="DeadLetter"/> from the actor system's
    /// <see cref="EventStream"/> into a <see cref="TappedMessage"/> with
    /// <see cref="TappedMessage.IsDeadLetter"/> set, then fan-out via the
    /// regular <see cref="Publish"/> path. Wrapped in try/catch because a
    /// broken <see cref="object.ToString"/> on a payload must never break
    /// the listener actor.
    /// </summary>
    internal void PublishDeadLetter(DeadLetter deadLetter)
    {
        try
        {
            var msg = deadLetter.Message;
            Publish(new TappedMessage(
                Recipient: _deadLetterPath,
                // Same marker the mailbox tap uses for "no sender" (#34), so the
                // two producers of one stream say the same thing.
                Sender: deadLetter.Sender?.Path?.ToString() ?? "<deadLetters>",
                MessageType: msg?.GetType().FullName ?? "<null>",
                Payload: msg?.ToString() ?? string.Empty,
                Timestamp: DateTime.UtcNow,
                IsDeadLetter: true));
        }
        catch
        {
            // Diagnostics must never crash the actor system. Swallow.
        }
    }

#pragma warning disable CA1812 // Instantiated by Akka via Props.Create
    /// <summary>
    /// Tiny internal actor that bridges the <see cref="EventStream"/>'s
    /// actor-based subscription API back into the
    /// <see cref="BowireAkkaExtension"/>'s plain method call. Holding a
    /// reference to the extension is fine: the extension's lifetime is the
    /// actor system's.
    /// </summary>
    private sealed class DeadLetterListener : UntypedActor
    {
        private readonly BowireAkkaExtension _extension;

        public DeadLetterListener(BowireAkkaExtension extension)
        {
            _extension = extension;
        }

        protected override void OnReceive(object message)
        {
            if (message is DeadLetter dl)
            {
                _extension.PublishDeadLetter(dl);
            }
        }
    }
#pragma warning restore CA1812
}

/// <summary>
/// Standard Akka.NET extension provider — load via
/// <c>BowireAkkaExtensionProvider.Instance.Apply(system)</c> or
/// <c>system.WithExtension&lt;BowireAkkaExtension, BowireAkkaExtensionProvider&gt;()</c>.
/// </summary>
public sealed class BowireAkkaExtensionProvider : ExtensionIdProvider<BowireAkkaExtension>
{
    /// <summary>Singleton id used by Akka's extension lookup.</summary>
    public static readonly BowireAkkaExtensionProvider Instance = new();

    /// <inheritdoc />
    public override BowireAkkaExtension CreateExtension(ExtendedActorSystem system) =>
        new(system);
}
