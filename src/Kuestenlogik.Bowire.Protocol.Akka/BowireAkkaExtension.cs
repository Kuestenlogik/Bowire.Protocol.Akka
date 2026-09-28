// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
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
/// implementation) call <see cref="Subscribe(TapFilter?, out object)"/> to get a fresh
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
    private readonly List<Subscriber> _subscribers = [];

    // A reader's channel and what it asked to see (#31). The filter is null
    // for "everything", so the common case costs no call.
    private sealed record Subscriber(Channel<TappedMessage> Channel, TapFilter? Filter, PayloadFormat Payload);
    private readonly List<ThroughputCounter> _counters = [];
    // Every live tap mailbox by owner path (#28). Filled at actor creation,
    // emptied at actor stop — nothing per message.
    private readonly ConcurrentDictionary<string, BowireTapMessageQueue> _mailboxes = new(StringComparer.Ordinal);
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
        TellPolicy = ReadTellPolicy(system);

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
    /// What the workbench may send into this actor system (#32), or null —
    /// the default — when it may send nothing.
    /// </summary>
    public TellPolicy? TellPolicy { get; private set; }

    /// <summary>
    /// Let the workbench send the listed message types to the listed actors
    /// (#32). Off until called; the workbench cannot turn it on.
    /// </summary>
    public void EnableTell(TellPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        TellPolicy = policy;
        System.Log.Warning(
            "Bowire: the workbench may send {0} to {1}.",
            string.Join(", ", policy.MessageTypes.Keys), string.Join(", ", policy.Paths));
    }

    /// <summary>Take the permission back; open Tell channels refuse from now on.</summary>
    public void DisableTell() => TellPolicy = null;

    internal void AuditTell(TellTarget target) =>
        // Every tell leaves a line in the system's own log: who asked is the
        // workbench, what and where is here.
        System.Log.Info("Bowire: told {0} a {1}", target.Path, target.Message.GetType().FullName);

    /// <summary>
    /// The policy from <c>bowire.akka.tell { paths = [...], message-types = [...] }</c>,
    /// or null. A type that does not resolve is logged and left out; a section
    /// that ends up allowing nothing enables nothing.
    /// </summary>
    private static TellPolicy? ReadTellPolicy(ExtendedActorSystem system)
    {
        try
        {
            var config = system.Settings.Config;
            if (!config.HasPath("bowire.akka.tell")) return null;
            var paths = config.GetStringList("bowire.akka.tell.paths") ?? [];
            var types = new List<Type>();
            foreach (var name in config.GetStringList("bowire.akka.tell.message-types") ?? [])
            {
                var type = Type.GetType(name, throwOnError: false);
                if (type is null) system.Log.Warning("Bowire: tell message type '{0}' does not resolve and is not allowed.", name);
                else types.Add(type);
            }
            if (paths.Count == 0 || types.Count == 0)
            {
                system.Log.Warning("Bowire: bowire.akka.tell allows no path or no type; Tell stays off.");
                return null;
            }
            var policy = new TellPolicy(paths, types);
            system.Log.Warning("Bowire: the workbench may send {0} to {1}.",
                string.Join(", ", policy.MessageTypes.Keys), string.Join(", ", policy.Paths));
            return policy;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            system.Log.Warning("Bowire: bowire.akka.tell could not be read ({0}); Tell stays off.", ex.Message);
            return null;
        }
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

    internal void RegisterMailbox(string path, BowireTapMessageQueue queue) => _mailboxes[path] = queue;

    // Only this queue: an actor re-created under the same name has already
    // put its new mailbox in, and the old one's clean-up must not take it out.
    internal void UnregisterMailbox(string path, BowireTapMessageQueue queue) =>
        _mailboxes.TryRemove(new KeyValuePair<string, BowireTapMessageQueue>(path, queue));

    /// <summary>
    /// The tap mailboxes as they are now: depth and the first
    /// <paramref name="head"/> queued messages of each, deepest first (#28).
    /// Nothing is taken off a queue.
    /// </summary>
    /// <param name="pathPrefix">Only actors whose path starts with this; null for all.</param>
    /// <param name="head">How many queued messages to show per mailbox.</param>
    /// <param name="nonEmptyOnly">Leave out mailboxes with nothing queued.</param>
    public IReadOnlyList<MailboxSnapshot> SnapshotMailboxes(string? pathPrefix, int head, bool nonEmptyOnly)
    {
        var result = new List<MailboxSnapshot>();
        foreach (var (path, queue) in _mailboxes)
        {
            if (pathPrefix is not null && !path.StartsWith(pathPrefix, StringComparison.Ordinal)) continue;
            var depth = queue.Count;
            if (nonEmptyOnly && depth == 0) continue;
            var queued = queue.Peek(head).Select(Describe).ToList();
            result.Add(new MailboxSnapshot(path, depth, queued));
        }
        return [.. result.OrderByDescending(m => m.Depth).ThenBy(m => m.Path, StringComparer.Ordinal)];
    }

    private static QueuedMessage Describe(Envelope envelope)
    {
        var msg = envelope.Message;
        string payload;
        // Another actor's message, rendered from this thread: a ToString that
        // throws shows as its type, it does not fail the whole view.
        try { payload = msg?.ToString() ?? string.Empty; }
        catch (Exception ex) when (ex is not OutOfMemoryException) { payload = $"<{ex.GetType().Name} in ToString>"; }
        return new QueuedMessage(
            MessageType: msg?.GetType().FullName ?? "<null>",
            Sender: envelope.Sender?.Path?.ToString() ?? "<deadLetters>",
            Payload: payload);
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
    public ChannelReader<TappedMessage> Subscribe(out object token) => Subscribe(null, PayloadFormat.None, out token);

    /// <inheritdoc cref="Subscribe(TapFilter?, PayloadFormat, out object)"/>
    public ChannelReader<TappedMessage> Subscribe(TapFilter? filter, out object token) => Subscribe(filter, PayloadFormat.None, out token);

    /// <summary>
    /// Open a fresh reader that only receives what <paramref name="filter"/>
    /// lets through (#31). The filter runs before the channel, so what it
    /// rejects never takes one of the reader's slots. With a
    /// <paramref name="payload"/> format other than <see cref="PayloadFormat.None"/>
    /// each message also carries <see cref="TappedMessage.PayloadJson"/> (#30)
    /// — rendered once per message and format however many readers ask, and
    /// not at all when none does.
    /// </summary>
    public ChannelReader<TappedMessage> Subscribe(TapFilter? filter, PayloadFormat payload, out object token)
    {
        var ch = Channel.CreateBounded<TappedMessage>(new BoundedChannelOptions(capacity: 1024)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
        var subscriber = new Subscriber(ch, filter is { IsEmpty: false } ? filter : null, payload);
        lock (_lock)
        {
            _subscribers.Add(subscriber);
        }
        EnsureDeadLetterBridge();
        token = subscriber;
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

    /// <summary>Tear-down counterpart to <see cref="Subscribe(TapFilter?, out object)"/>.</summary>
    public void Unsubscribe(object token)
    {
        if (token is not Subscriber subscriber) return;
        lock (_lock)
        {
            _subscribers.Remove(subscriber);
        }
        subscriber.Channel.Writer.TryComplete();
    }

    /// <summary>
    /// Called by <see cref="BowireTapMessageQueue.Enqueue"/> for every
    /// tapped message. Fan-out: written to each subscriber's channel
    /// without awaiting (drop-oldest takes care of slow consumers).
    /// </summary>
    /// <param name="msg">The observation.</param>
    /// <param name="original">The message itself, for a reader that wants it structured (#30).</param>
    internal void Publish(TappedMessage msg, object? original)
    {
        // Snapshot under lock; write outside to keep the hot path short.
        Subscriber[] snapshot;
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
        // One rendering per format per message, made on first demand.
        TappedMessage?[]? rendered = null;
        foreach (var subscriber in snapshot)
        {
            if (subscriber.Filter is { } filter)
            {
                // A pattern that times out on some odd path is that message
                // not shown — never the tap throwing into the sender.
                bool pass;
                try { pass = filter.Matches(msg); }
                catch (System.Text.RegularExpressions.RegexMatchTimeoutException) { pass = false; }
                if (!pass) continue;
            }
            if (subscriber.Payload != PayloadFormat.None && original is not null)
            {
                rendered ??= new TappedMessage?[Enum.GetValues<PayloadFormat>().Length];
                var slot = (int)subscriber.Payload;
                rendered[slot] ??= msg with { PayloadJson = PayloadRenderer.Render(System, original, subscriber.Payload) };
                subscriber.Channel.Writer.TryWrite(rendered[slot]!);
            }
            else
            {
                subscriber.Channel.Writer.TryWrite(msg);
            }
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
                IsDeadLetter: true), msg);
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
