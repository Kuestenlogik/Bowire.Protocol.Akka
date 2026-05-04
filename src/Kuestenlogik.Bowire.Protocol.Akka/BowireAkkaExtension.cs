// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// Akka.NET extension that owns the per-actor-system tap state for the
/// Bowire workbench: an unbounded broadcast channel that
/// <see cref="BowireTapMailbox"/> writes every tapped enqueue into, plus
/// a small filter set that decides which messages are worth forwarding.
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
    private readonly IActorRef? _deadLetterListener;
    private readonly string _deadLetterPath;

    /// <summary>The actor system this extension instance belongs to.</summary>
    public ExtendedActorSystem System { get; }

    internal BowireAkkaExtension(ExtendedActorSystem system)
    {
        System = system;
        _deadLetterPath = system.DeadLetters.Path.ToString();

        // Spawn a private system actor that bridges the EventStream's
        // actor-based DeadLetter notifications to PublishDeadLetter.
        //
        // Wrapped in try/catch: if the BowireTapMailbox is configured
        // as the *global* default mailbox (akka.actor.default-mailbox),
        // the mailbox is created for the root guardian during bootstrap
        // and triggers Apply on this extension before the actor system
        // is itself navigable. SystemActorOf NREs in that path. The
        // surgical opt-in pattern (per-actor `Props.WithMailbox` or a
        // named mailbox config like `akka.actor.bowire-tap`) avoids the
        // bootstrap entanglement, but we degrade gracefully so a global
        // default-mailbox swap still gives you the live tap stream
        // (just without dead-letter capture).
        try
        {
            _deadLetterListener = system.SystemActorOf(
                Props.Create(() => new DeadLetterListener(this)),
                "bowire-deadletter-listener");
            system.EventStream.Subscribe(_deadLetterListener, typeof(DeadLetter));
        }
        catch
        {
            // System not navigable yet (root-guardian bootstrap path).
            // Live mailbox taps still work; dead-letter capture is the
            // only thing missing in that mode.
            _deadLetterListener = null;
        }

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
    /// True when at least one Bowire subscriber is listening. Read by the
    /// tap mailbox on every enqueue to short-circuit the marshalling path
    /// when nobody's watching.
    /// </summary>
    public bool HasSubscribers
    {
        get
        {
            lock (_lock)
            {
                return _subscribers.Count > 0;
            }
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
        token = ch;
        return ch.Reader;
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
        lock (_lock)
        {
            if (_subscribers.Count == 0) return;
            snapshot = _subscribers.ToArray();
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
                Sender: deadLetter.Sender?.Path?.ToString() ?? string.Empty,
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
