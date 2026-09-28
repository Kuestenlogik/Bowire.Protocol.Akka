// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Configuration;
using Akka.Dispatch;
using Akka.Dispatch.MessageQueues;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// Custom Akka.NET <see cref="MailboxType"/> that wraps the standard
/// unbounded message queue and reports every enqueue to
/// <see cref="BowireAkkaExtension"/>. Opt-in by either:
/// <list type="bullet">
///   <item>
///     Globally — set <c>akka.actor.default-mailbox.mailbox-type</c> to
///     this type's assembly-qualified name; every actor created after
///     that uses the tap.
///   </item>
///   <item>
///     Per-actor — define a custom mailbox section in HOCON
///     (<c>akka.actor.bowire-tap = { mailbox-type = "..." }</c>) and
///     attach it via <c>props.WithMailbox("akka.actor.bowire-tap")</c>.
///   </item>
/// </list>
/// </summary>
public sealed class BowireTapMailbox : MailboxType, IProducesMessageQueue<UnboundedMessageQueue>
{
    /// <summary>Standard MailboxType ctor — Akka resolves it via reflection.</summary>
    public BowireTapMailbox(Settings settings, Config config) : base(settings, config) { }

    /// <inheritdoc />
    public override IMessageQueue Create(IActorRef? owner, ActorSystem? system) =>
        new BowireTapMessageQueue(owner, system);
}

/// <summary>
/// An unbounded message queue that taps every enqueue into the system's
/// <see cref="BowireAkkaExtension"/>. The dequeue path is unmodified — the
/// tap is one-way (mailbox in only).
/// </summary>
/// <remarks>
/// It holds its own <see cref="ConcurrentQueue{T}"/> rather than wrapping
/// <see cref="UnboundedMessageQueue"/>, because that one keeps its queue
/// private and the mailbox view (#28) has to look at the head without taking
/// anything off it. Akka's <see cref="UnboundedMessageQueue"/> is exactly this
/// queue and these five members, so delivery behaves the same.
/// </remarks>
internal sealed class BowireTapMessageQueue : IMessageQueue, IUnboundedMessageQueueSemantics
{
    private readonly ConcurrentQueue<Envelope> _queue = new();
    private readonly BowireAkkaExtension? _extension;
    private readonly string _ownerPath;

    public BowireTapMessageQueue(IActorRef? owner, ActorSystem? system)
    {
        _ownerPath = owner?.Path?.ToString() ?? "<unattached>";
        // ActorSystem can be null when Akka calls Create for capability
        // probing during dispatcher resolution. The mailbox still has to
        // produce a queue — fall through with no extension; we'll skip
        // the tap on subsequent enqueues for that probe.
        if (system is ExtendedActorSystem ext)
        {
            _extension = BowireAkkaExtensionProvider.Instance.Apply(ext);
            // A queue with an owner is a real mailbox; one without is Akka
            // probing the mailbox type and never holds a message.
            if (owner is not null) _extension.RegisterMailbox(_ownerPath, this);
        }
    }

    /// <summary>Absolute path of the actor this mailbox belongs to.</summary>
    internal string OwnerPath => _ownerPath;

    /// <inheritdoc />
    public bool HasMessages => !_queue.IsEmpty;

    /// <inheritdoc />
    public int Count => _queue.Count;

    /// <summary>
    /// The first <paramref name="count"/> queued envelopes, oldest first,
    /// without taking them off. Enumerating a <see cref="ConcurrentQueue{T}"/>
    /// reads a moment-in-time snapshot: the actor goes on dequeuing, and
    /// nothing it receives is reordered or missed.
    /// </summary>
    internal List<Envelope> Peek(int count)
    {
        var head = new List<Envelope>(Math.Min(count, 64));
        if (count <= 0) return head;
        foreach (var envelope in _queue)
        {
            head.Add(envelope);
            if (head.Count >= count) break;
        }
        return head;
    }

    /// <inheritdoc />
    public void Enqueue(IActorRef receiver, Envelope envelope)
    {
        // Hot path: short-circuit when nobody's listening so the tap
        // costs ~one volatile read per message in steady state.
        if (_extension is { HasSubscribers: true })
        {
            try
            {
                var msg = envelope.Message;
                _extension.Publish(new TappedMessage(
                    Recipient: _ownerPath,
                    Sender: envelope.Sender?.Path?.ToString() ?? "<deadLetters>",
                    MessageType: msg?.GetType().FullName ?? "<null>",
                    Payload: msg?.ToString() ?? string.Empty,
                    Timestamp: DateTime.UtcNow));
            }
            catch
            {
                // The tap must never break message delivery. Swallow
                // anything that goes wrong while marshalling and let
                // the inner queue do its job.
            }
        }
        _queue.Enqueue(envelope);
    }

    /// <inheritdoc />
    public bool TryDequeue(out Envelope envelope) => _queue.TryDequeue(out envelope);

    /// <inheritdoc />
    public void CleanUp(IActorRef owner, IMessageQueue deadletters)
    {
        // The actor is gone: its mailbox leaves the view, and whatever was
        // still queued goes to dead letters, as with Akka's own queue.
        _extension?.UnregisterMailbox(_ownerPath, this);
        while (TryDequeue(out var msg))
        {
            deadletters.Enqueue(owner, msg);
        }
    }
}
