// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json;
using Akka.Actor;
using Akka.Configuration;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// A mailbox's depth and the messages waiting at its head (#28).
/// </summary>
/// <remarks>
/// Looking must not change what the actor gets: the head is read without
/// taking anything off, and the tests hold that a blocked actor, once
/// released, still processes every message in the order it was sent.
/// </remarks>
public sealed class MailboxSnapshotTests
{
    private const string Named = """
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        """;

    private static (ActorSystem System, BowireAkkaExtension Ext) Start(string name)
    {
        var system = ActorSystem.Create(name, ConfigurationFactory.ParseString(Named));
        return (system, BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system));
    }

    private static async Task<MailboxSnapshot> WaitForDepth(BowireAkkaExtension ext, IActorRef actor, int depth)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            var m = ext.SnapshotMailboxes(actor.Path.ToString(), head: 5, nonEmptyOnly: false).SingleOrDefault();
            if (m is not null && m.Depth == depth) return m;
            if (cts.IsCancellationRequested) throw new TimeoutException($"depth {m?.Depth}, expected {depth}");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    // An actor's cell, and with it the mailbox, is set up asynchronously after
    // ActorOf returns; the view shows it once it exists.
    private static async Task<int> WaitForCount(BowireAkkaExtension ext, string prefix, int expected)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        int n;
        while ((n = ext.SnapshotMailboxes(prefix, 0, false).Count) != expected && !cts.IsCancellationRequested)
            await Task.Delay(10, TestContext.Current.CancellationToken);
        return n;
    }

    [Fact]
    public async Task The_Head_Shows_What_Waits_And_The_Actor_Still_Gets_All_Of_It_In_Order()
    {
        var (system, ext) = Start("mb-order");
        using var _ = system;
        using var gate = new ManualResetEventSlim(false);
        var received = new ConcurrentQueue<int>();
        var actor = system.ActorOf(Props.Create(() => new GatedActor(gate, received)).WithMailbox("akka.actor.bowire-tap"), "gated");

        for (var i = 0; i < 10; i++) actor.Tell(i);

        // Message 0 is being processed (and blocks); 1..9 wait.
        var snapshot = await WaitForDepth(ext, actor, 9);
        Assert.Equal(["1", "2", "3", "4", "5"], snapshot.Head.Select(q => q.Payload));
        Assert.All(snapshot.Head, q => Assert.Equal(typeof(int).FullName, q.MessageType));

        // Looking twice changes nothing either.
        Assert.Equal(snapshot.Head, (await WaitForDepth(ext, actor, 9)).Head);

        gate.Set();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (received.Count < 10 && !cts.IsCancellationRequested) await Task.Delay(10, TestContext.Current.CancellationToken);
        Assert.Equal(Enumerable.Range(0, 10), received);
    }

    [Fact]
    public async Task A_Stopped_Actor_Leaves_The_View()
    {
        var (system, ext) = Start("mb-stop");
        using var _ = system;
        var actor = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "short-lived");
        Assert.Equal(1, await WaitForCount(ext, actor.Path.ToString(), 1));

        await actor.GracefulStop(TimeSpan.FromSeconds(3));
        Assert.Equal(0, await WaitForCount(ext, actor.Path.ToString(), 0));
    }

    [Fact]
    public async Task An_Actor_Recreated_Under_The_Same_Name_Stays_In_The_View()
    {
        // The old mailbox's clean-up can run after the new one registered; it
        // must take out itself, not its successor.
        var (system, ext) = Start("mb-recreate");
        using var _ = system;
        var first = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "again");
        await first.GracefulStop(TimeSpan.FromSeconds(3));
        var second = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "again");
        Assert.Equal(1, await WaitForCount(ext, second.Path.ToString(), 1));
        // And it stays: nothing late takes it out again.
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.Single(ext.SnapshotMailboxes(second.Path.ToString(), 0, false));
    }

    [Fact]
    public async Task The_Filters_Narrow_By_Path_And_Leave_Out_Empty_Mailboxes()
    {
        var (system, ext) = Start("mb-filter");
        using var _ = system;
        using var gate = new ManualResetEventSlim(false);
        var busy = system.ActorOf(Props.Create(() => new GatedActor(gate, new ConcurrentQueue<int>())).WithMailbox("akka.actor.bowire-tap"), "dock-busy");
        system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "dock-idle");
        system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "harbor-master");
        busy.Tell(1); busy.Tell(2);
        await WaitForDepth(ext, busy, 1);
        await WaitForCount(ext, "akka://mb-filter/user/", 3);

        var docks = ext.SnapshotMailboxes("akka://mb-filter/user/dock", 0, false);
        Assert.Equal(2, docks.Count);
        Assert.Equal(busy.Path.ToString(), docks[0].Path);           // deepest first

        var queued = ext.SnapshotMailboxes(null, 0, nonEmptyOnly: true);
        Assert.Equal(busy.Path.ToString(), Assert.Single(queued).Path);
        gate.Set();
    }

    [Fact]
    public async Task The_Mailboxes_Method_Answers_In_The_Invoke_Pane()
    {
        var (system, _) = Start("mb-invoke");
        using var __ = system;
        system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "one");
        await WaitForCount(BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system), "akka://mb-invoke/user/", 1);
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(new SingleService(system));

        var result = await plugin.InvokeAsync("akka://embedded", BowireAkkaProtocol.TapServiceName,
            BowireAkkaProtocol.MailboxesMethodName, ["""{ "path": "akka://mb-invoke/user/" }"""], false,
            ct: TestContext.Current.CancellationToken);

        Assert.Equal("OK", result.Status);
        using var doc = JsonDocument.Parse(result.Response!);
        var entry = Assert.Single(doc.RootElement.EnumerateArray());
        Assert.Equal("akka://mb-invoke/user/one", entry.GetProperty("Path").GetString());
        Assert.Equal(0, entry.GetProperty("Depth").GetInt32());
    }

    [Theory]
    [InlineData(null, null, 5, false)]
    [InlineData("""{ "path": "akka://x/user", "head": 500, "nonEmptyOnly": true }""", "akka://x/user", 100, true)]
    [InlineData("""{ "path": "", "head": -3 }""", null, 0, false)]
    [InlineData("[]", null, 5, false)]
    public void A_Request_Out_Of_Range_Falls_Back_Rather_Than_Failing(string? body, string? path, int head, bool nonEmpty)
    {
        Assert.Equal((path, head, nonEmpty), BowireAkkaProtocol.ReadMailboxesRequest(body is null ? [] : [body]));
    }

    private sealed class SingleService(ActorSystem system) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(ActorSystem) ? system : null;
    }

#pragma warning disable CA1812 // Akka instantiates these via reflection in Props.Create<T>()
    private sealed class SinkActor : UntypedActor
    {
        protected override void OnReceive(object message) { }
    }

    private sealed class GatedActor(ManualResetEventSlim gate, ConcurrentQueue<int> received) : UntypedActor
    {
        protected override void OnReceive(object message)
        {
            gate.Wait(TimeSpan.FromSeconds(10));
            if (message is int i) received.Enqueue(i);
        }
    }
#pragma warning restore CA1812
}
