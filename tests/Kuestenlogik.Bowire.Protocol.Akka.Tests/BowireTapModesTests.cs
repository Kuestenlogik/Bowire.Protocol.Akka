// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;
using Akka.Configuration;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// The tap under the wiring modes the README offers, and with more than one
/// reader (#33, #36).
/// </summary>
/// <remarks>
/// COVERAGE.md marked the global-default mode as tested while every mailbox
/// test used <c>Props.WithMailbox</c>. That mode is also the one in which
/// dead-letter capture was silently off (#33) — the listener was spawned
/// while the root guardian was still being built, the spawn threw, and a
/// catch-all hid it. So the modes get tests of their own, and the one that
/// was quietly broken gets the test that shows it works.
/// </remarks>
public sealed class BowireTapModesTests
{
    private const string GlobalDefault = """
        akka.actor.default-mailbox.mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        """;

    private const string Named = """
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        """;

    private static async Task<TappedMessage?> ReadUntil(
        System.Threading.Channels.ChannelReader<TappedMessage> reader, Func<TappedMessage, bool> match)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            while (true)
            {
                var got = await reader.ReadAsync(cts.Token);
                if (match(got)) return got;
            }
        }
        catch (OperationCanceledException) { return null; }
    }

    [Fact]
    public async Task Global_Default_Mailbox_Taps_An_Ordinary_Actor()
    {
        using var system = ActorSystem.Create("tap-global", ConfigurationFactory.ParseString(GlobalDefault));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(out var token);
        try
        {
            var echo = system.ActorOf(Props.Create<EchoActor>(), "echo");
            echo.Tell("hello-global");

            var tap = await ReadUntil(reader, m => m.Payload == "hello-global");
            Assert.NotNull(tap);
            Assert.Equal(echo.Path.ToString(), tap!.Recipient);
            Assert.False(tap.IsDeadLetter);
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task Global_Default_Mailbox_Captures_Dead_Letters_Too()
    {
        // #33 — the mode in which this used to be silently off.
        using var system = ActorSystem.Create("tap-global-dl", ConfigurationFactory.ParseString(GlobalDefault));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(out var token);
        try
        {
            var doomed = system.ActorOf(Props.Create<EchoActor>(), "doomed");
            await doomed.GracefulStop(TimeSpan.FromSeconds(2));
            doomed.Tell("orphan-global");

            var dl = await ReadUntil(reader, m => m.IsDeadLetter && m.Payload == "orphan-global");
            Assert.NotNull(dl);
            Assert.Equal(system.DeadLetters.Path.ToString(), dl!.Recipient);
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task Mixed_Wiring_Taps_Both_Kinds_Of_Actor_Once_Each()
    {
        // #36 — the global default plus an actor that asks for the named tap
        // mailbox explicitly. Both are tap mailboxes; neither message may be
        // lost, and neither may arrive twice.
        using var system = ActorSystem.Create("tap-mixed",
            ConfigurationFactory.ParseString(GlobalDefault + "\n" + Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(out var token);
        try
        {
            var byDefault = system.ActorOf(Props.Create<EchoActor>(), "by-default");
            var byName = system.ActorOf(Props.Create<EchoActor>().WithMailbox("akka.actor.bowire-tap"), "by-name");
            byDefault.Tell("from-default");
            byName.Tell("from-named");

            var seen = new List<TappedMessage>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            try
            {
                while (true) seen.Add(await reader.ReadAsync(cts.Token));
            }
            catch (OperationCanceledException) { }

            Assert.Single(seen, m => m.Payload == "from-default" && m.Recipient == byDefault.Path.ToString());
            Assert.Single(seen, m => m.Payload == "from-named" && m.Recipient == byName.Path.ToString());
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task Two_Readers_Each_Get_Every_Message()
    {
        // #36 — the many-subscriber path. Each reader has its own channel, so
        // one reading does not take a message away from the other.
        using var system = ActorSystem.Create("tap-two-readers", ConfigurationFactory.ParseString(Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var first = ext.Subscribe(out var t1);
        var second = ext.Subscribe(out var t2);
        try
        {
            var echo = system.ActorOf(Props.Create<EchoActor>().WithMailbox("akka.actor.bowire-tap"), "echo");
            echo.Tell("for-both");

            Assert.NotNull(await ReadUntil(first, m => m.Payload == "for-both"));
            Assert.NotNull(await ReadUntil(second, m => m.Payload == "for-both"));
        }
        finally
        {
            ext.Unsubscribe(t1);
            ext.Unsubscribe(t2);
        }
    }

    [Fact]
    public async Task A_Reader_That_Leaves_Stops_Receiving_And_The_Other_Carries_On()
    {
        // 2 → 1 → 0: the channel of the one that left is completed, and the
        // extension goes back to "nobody is watching".
        using var system = ActorSystem.Create("tap-leave", ConfigurationFactory.ParseString(Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var staying = ext.Subscribe(out var keep);
        var leaving = ext.Subscribe(out var drop);
        var echo = system.ActorOf(Props.Create<EchoActor>().WithMailbox("akka.actor.bowire-tap"), "echo");

        ext.Unsubscribe(drop);
        echo.Tell("after-leave");

        Assert.NotNull(await ReadUntil(staying, m => m.Payload == "after-leave"));
        // The leaver's channel is completed, so its consumer loop ends instead of hanging.
        await leaving.Completion.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        Assert.True(leaving.Completion.IsCompleted);
        Assert.True(ext.HasSubscribers);

        ext.Unsubscribe(keep);
        Assert.False(ext.HasSubscribers);
    }

#pragma warning disable CA1812 // Akka instantiates these via reflection in Props.Create<T>()
    private sealed class EchoActor : UntypedActor
    {
        protected override void OnReceive(object message) { /* the tap is what is under test */ }
    }
#pragma warning restore CA1812
}
