// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Threading.Channels;
using Akka.Actor;

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

    /// <summary>The actor system this extension instance belongs to.</summary>
    public ExtendedActorSystem System { get; }

    internal BowireAkkaExtension(ExtendedActorSystem system)
    {
        System = system;
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
