// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Akka.Actor;
using Kuestenlogik.Bowire.Models;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>A message an actor sent back to the workbench.</summary>
/// <param name="From">Absolute path of the actor that replied.</param>
/// <param name="MessageType">CLR type name of the reply.</param>
/// <param name="Payload">The reply's <see cref="object.ToString"/>.</param>
/// <param name="PayloadJson">The reply as JSON (the <c>auto</c> rendering of #30), or null.</param>
/// <param name="Timestamp">UTC time the reply arrived.</param>
public sealed record TellReply(string From, string MessageType, string Payload, JsonElement? PayloadJson, DateTime Timestamp);

/// <summary>
/// The duplex Tell channel (#32): each message sent is a tell request, and
/// what the told actors send back arrives on the response side.
/// </summary>
/// <remarks>
/// The tells go out from an actor of their own, created for the channel and
/// stopped with it, so a reply to <c>Sender</c> comes back here rather than
/// to dead letters. A refused request is answered on the response side with
/// the reason, and <see cref="SendAsync"/> returns false.
/// </remarks>
internal sealed class BowireTellChannel : IBowireChannel
{
    private readonly ExtendedActorSystem _system;
    private readonly BowireAkkaExtension _extension;
    private readonly Channel<string> _responses = Channel.CreateBounded<string>(
        new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
    private readonly IActorRef _replyTo;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private int _sent;
    private bool _closed;

    public BowireTellChannel(ExtendedActorSystem system, BowireAkkaExtension extension)
    {
        _system = system;
        _extension = extension;
        Id = Guid.NewGuid().ToString("N");
        _replyTo = system.SystemActorOf(Props.Create(() => new ReplyCollector(system, _responses.Writer)), $"bowire-tell-{Id}");
    }

    public string Id { get; }
    public bool IsClientStreaming => true;
    public bool IsServerStreaming => true;
    public int SentCount => Volatile.Read(ref _sent);
    public bool IsClosed => _closed;
    public long ElapsedMs => _clock.ElapsedMilliseconds;

    public Task<bool> SendAsync(string jsonMessage, CancellationToken ct = default)
    {
        if (_closed) return Task.FromResult(false);
        var (target, refusal) = TellGateway.Resolve(_system, _extension.TellPolicy, jsonMessage);
        if (target is null)
        {
            _responses.Writer.TryWrite(JsonSerializer.Serialize(new { refused = refusal }));
            return Task.FromResult(false);
        }
        _extension.AuditTell(target);
        target.Selection.Tell(target.Message, _replyTo);
        Interlocked.Increment(ref _sent);
        return Task.FromResult(true);
    }

    public Task CloseAsync(CancellationToken ct = default)
    {
        // Only the send side: replies to what was already sent still arrive.
        _closed = true;
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<string> ReadResponsesAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var response in _responses.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            yield return response;
    }

    public ValueTask DisposeAsync()
    {
        _closed = true;
        _system.Stop(_replyTo);
        _responses.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }

#pragma warning disable CA1812 // instantiated by Akka via Props.Create
    private sealed class ReplyCollector(ExtendedActorSystem system, ChannelWriter<string> writer) : UntypedActor
    {
        protected override void OnReceive(object message)
        {
            try
            {
                writer.TryWrite(JsonSerializer.Serialize(new TellReply(
                    From: Sender?.Path?.ToString() ?? "<deadLetters>",
                    MessageType: message.GetType().FullName ?? "<null>",
                    Payload: message.ToString() ?? string.Empty,
                    PayloadJson: PayloadRenderer.Render(system, message, PayloadFormat.Auto),
                    Timestamp: DateTime.UtcNow)));
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // A reply that cannot be shown is not a reason to stop the channel.
            }
        }
    }
#pragma warning restore CA1812
}
