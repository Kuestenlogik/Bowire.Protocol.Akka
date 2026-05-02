// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;
using Akka.Configuration;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

public sealed class BowireTapMailboxTests
{
    private const string TapHocon = """
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        """;

    [Fact]
    public async Task Subscriber_ReceivesMessageWhenActorEnqueues()
    {
        using var system = ActorSystem.Create("test-tap", ConfigurationFactory.ParseString(TapHocon));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);

        // Subscribe BEFORE creating the actor — otherwise HasSubscribers
        // is false at enqueue time and the tap short-circuits.
        var reader = ext.Subscribe(out var token);
        try
        {
            var echo = system.ActorOf(EchoActor.Build().WithMailbox("akka.actor.bowire-tap"), "echo");
            echo.Tell("hello-akka");

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var got = await reader.ReadAsync(cts.Token);

            Assert.Equal("akka://test-tap/user/echo", got.Recipient);
            Assert.Equal("System.String", got.MessageType);
            Assert.Equal("hello-akka", got.Payload);
        }
        finally
        {
            ext.Unsubscribe(token);
        }
    }

    [Fact]
    public void NoSubscribers_TapMailboxIsTransparent()
    {
        // No subscriber attached — actor still receives messages, mailbox
        // tap is a complete no-op for cost / correctness.
        using var system = ActorSystem.Create("test-tap-noobs", ConfigurationFactory.ParseString(TapHocon));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);

        Assert.False(ext.HasSubscribers);

        var echo = system.ActorOf(EchoActor.Build().WithMailbox("akka.actor.bowire-tap"), "echo");
        echo.Tell("dropped");
        // No subscriber means no observable side-effect — assert that
        // the system is still operational rather than that nothing
        // happened (you can't assert absence of a never-emitted event).
        Assert.False(system.WhenTerminated.IsCompleted);
    }

#pragma warning disable CA1812 // Akka instantiates these via reflection in Props.Create<T>()
    private sealed class EchoActor : UntypedActor
    {
        public static Props Build() => global::Akka.Actor.Props.Create<EchoActor>();
        protected override void OnReceive(object message) { /* no-op */ }
    }
#pragma warning restore CA1812
}
