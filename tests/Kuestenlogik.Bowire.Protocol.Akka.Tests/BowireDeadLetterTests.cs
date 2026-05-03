// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

public sealed class BowireDeadLetterTests
{
    [Fact]
    public async Task Subscriber_ReceivesDeadLetter_FromEventStream()
    {
        using var system = ActorSystem.Create("test-deadletter");
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);

        var reader = ext.Subscribe(out var token);
        try
        {
            // Tell to a terminated actor → Akka publishes a DeadLetter on
            // the EventStream. We stop the actor and then send to it; the
            // mailbox is gone so the message can't be delivered.
            var doomed = system.ActorOf(NoopActor.Build(), "doomed");
            await doomed.GracefulStop(TimeSpan.FromSeconds(2));
            doomed.Tell("orphan-message");

            // Read messages until we find one flagged as a dead letter
            // (the gracefulStop poison-pill itself doesn't end up as a
            // dead letter, but defensive filtering keeps the assertion
            // crisp).
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            TappedMessage? deadLetter = null;
            while (!cts.IsCancellationRequested)
            {
                var got = await reader.ReadAsync(cts.Token);
                if (got.IsDeadLetter && got.Payload == "orphan-message")
                {
                    deadLetter = got;
                    break;
                }
            }

            Assert.NotNull(deadLetter);
            Assert.True(deadLetter!.IsDeadLetter);
            Assert.Equal("System.String", deadLetter.MessageType);
            Assert.Equal("orphan-message", deadLetter.Payload);
            Assert.Equal(system.DeadLetters.Path.ToString(), deadLetter.Recipient);
        }
        finally
        {
            ext.Unsubscribe(token);
        }
    }

    [Fact]
    public void RegularTappedMessage_HasIsDeadLetterFalseByDefault()
    {
        // Backwards-compatibility check — the new optional parameter
        // defaults to false so callers from before 0.2.0 keep working.
        var msg = new TappedMessage(
            Recipient: "akka://test/user/foo",
            Sender: "akka://test/user/bar",
            MessageType: "System.String",
            Payload: "hi",
            Timestamp: DateTime.UtcNow);

        Assert.False(msg.IsDeadLetter);
    }

#pragma warning disable CA1812 // Akka instantiates these via reflection in Props.Create<T>()
    private sealed class NoopActor : UntypedActor
    {
        public static Props Build() => global::Akka.Actor.Props.Create<NoopActor>();
        protected override void OnReceive(object message) { /* no-op */ }
    }
#pragma warning restore CA1812
}
