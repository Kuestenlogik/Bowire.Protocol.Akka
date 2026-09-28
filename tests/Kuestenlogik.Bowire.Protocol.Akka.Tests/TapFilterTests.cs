// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Configuration;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// Filtering the tap per actor path and message type (#31).
/// </summary>
/// <remarks>
/// The extension's XML doc once claimed a "filter set" that never existed.
/// This is the real one, and it filters before the subscriber's channel: the
/// last extension test shows why that matters — a message that was asked for
/// survives a flood of ones that were not.
/// </remarks>
public sealed class TapFilterTests
{
    private const string Named = """
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        """;

    private static TappedMessage Msg(string recipient, string type, bool dead = false) =>
        new(recipient, "<deadLetters>", type, "x", DateTime.UtcNow, dead);

    // ---- the filter ----

    [Theory]
    [InlineData("akka://Harbor/user/dock-*", "akka://Harbor/user/dock-1", true)]
    [InlineData("/user/dock-*", "akka://Harbor/user/dock-1", true)]
    [InlineData("/user/dock-*", "akka.tcp://Harbor@10.0.0.1:4053/user/dock-7", true)]
    [InlineData("/user/dock-*", "akka://Harbor/user/harbor-master", false)]
    [InlineData("/user/dock", "akka://Harbor/user/dock-1", false)]      // whole value, not a prefix
    [InlineData("/user/*", "akka://Harbor/user/dock-1/crane", true)]    // * crosses segments
    public void A_Path_Pattern_Matches_The_Full_Path_Or_The_Path_From_The_Root(string pattern, string path, bool expected)
    {
        Assert.Equal(expected, new TapFilter(paths: [pattern]).Matches(Msg(path, "M")));
    }

    [Theory]
    [InlineData("Harbor.Messages.PortCallClosed", true)]
    [InlineData("PortCallClosed", true)]
    [InlineData("PortCall*", true)]
    [InlineData("Harbor.Messages.*", true)]
    [InlineData("PortCallOpened", false)]
    public void A_Type_Pattern_Matches_The_Full_Or_The_Simple_Name(string pattern, bool expected)
    {
        Assert.Equal(expected, new TapFilter(messageTypes: [pattern]).Matches(Msg("akka://H/user/a", "Harbor.Messages.PortCallClosed")));
    }

    [Fact]
    public void A_Nested_Type_Is_Known_By_Its_Own_Name()
    {
        Assert.True(new TapFilter(messageTypes: ["Arrived"]).Matches(Msg("akka://H/user/a", "Harbor.Dock+Arrived")));
    }

    [Fact]
    public void Patterns_Of_One_Kind_Are_Alternatives_And_The_Two_Kinds_Must_Both_Match()
    {
        var filter = new TapFilter(paths: ["/user/a", "/user/b"], messageTypes: ["Ping"]);
        Assert.True(filter.Matches(Msg("akka://H/user/b", "Ping")));
        Assert.False(filter.Matches(Msg("akka://H/user/b", "Pong")));
        Assert.False(filter.Matches(Msg("akka://H/user/c", "Ping")));
    }

    [Fact]
    public void Dead_Letters_Can_Be_Left_Out()
    {
        var filter = new TapFilter(includeDeadLetters: false);
        Assert.False(filter.IsEmpty);
        Assert.False(filter.Matches(Msg("akka://H/deadLetters", "M", dead: true)));
        Assert.True(filter.Matches(Msg("akka://H/user/a", "M")));
    }

    [Fact]
    public void Blank_Patterns_Place_No_Restriction()
    {
        Assert.True(new TapFilter(paths: ["", "  "], messageTypes: []).IsEmpty);
    }

    [Fact]
    public void Regex_Characters_In_A_Pattern_Are_Literal()
    {
        var filter = new TapFilter(paths: ["/user/a.b"]);
        Assert.True(filter.Matches(Msg("akka://H/user/a.b", "M")));
        Assert.False(filter.Matches(Msg("akka://H/user/aXb", "M")));
    }

    // ---- the request ----

    [Fact]
    public void The_Request_Takes_A_List_Or_A_Single_Pattern()
    {
        var filter = BowireAkkaProtocol.ReadMonitorRequest(["""{ "paths": "/user/dock-*", "messageTypes": ["Ping", "Pong"], "includeDeadLetters": false }"""])!;
        Assert.Equal(["/user/dock-*"], filter.Paths);
        Assert.Equal(["Ping", "Pong"], filter.MessageTypes);
        Assert.False(filter.IncludeDeadLetters);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{}")]
    [InlineData("""{ "paths": [] }""")]
    [InlineData("not json")]
    [InlineData("[1, 2]")]
    public void A_Request_That_Asks_For_Nothing_In_Particular_Gets_Everything(string? body)
    {
        Assert.Null(BowireAkkaProtocol.ReadMonitorRequest(body is null ? [] : [body]));
    }

    // ---- at the tap ----

    private static async Task<List<TappedMessage>> Drain(ChannelReader<TappedMessage> reader, TimeSpan quiet)
    {
        var got = new List<TappedMessage>();
        while (true)
        {
            using var cts = new CancellationTokenSource(quiet);
            try { got.Add(await reader.ReadAsync(cts.Token)); }
            catch (OperationCanceledException) { return got; }
        }
    }

    [Fact]
    public async Task A_Filtered_Reader_Gets_Its_Actors_While_An_Unfiltered_One_Gets_All()
    {
        using var system = ActorSystem.Create("filter-two", ConfigurationFactory.ParseString(Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var docks = ext.Subscribe(new TapFilter(paths: ["/user/dock-*"]), out var t1);
        var all = ext.Subscribe(out var t2);
        try
        {
            var dock = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "dock-1");
            var master = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "harbor-master");
            dock.Tell("to-dock");
            master.Tell("to-master");

            var fromDocks = await Drain(docks, TimeSpan.FromMilliseconds(500));
            var fromAll = await Drain(all, TimeSpan.FromMilliseconds(200));
            Assert.Equal(["to-dock"], fromDocks.Select(m => m.Payload));
            Assert.Contains(fromAll, m => m.Payload == "to-master");
            Assert.Contains(fromAll, m => m.Payload == "to-dock");
        }
        finally
        {
            ext.Unsubscribe(t1);
            ext.Unsubscribe(t2);
        }
    }

    [Fact]
    public async Task What_Was_Asked_For_Survives_A_Flood_Of_What_Was_Not()
    {
        // Unfiltered, the rare message would be the oldest of 5001 in a
        // 1024-slot drop-oldest channel — gone before anybody reads.
        using var system = ActorSystem.Create("filter-flood", ConfigurationFactory.ParseString(Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var rare = ext.Subscribe(new TapFilter(messageTypes: ["String"]), out var token);
        try
        {
            var sink = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "sink");
            sink.Tell("the-one");
            for (var i = 0; i < 5000; i++) sink.Tell(i);
            await Task.Delay(500, TestContext.Current.CancellationToken);

            var got = await Drain(rare, TimeSpan.FromMilliseconds(300));
            Assert.Equal(["the-one"], got.Select(m => m.Payload));
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task The_Monitor_Stream_Applies_The_Filter_From_Its_Request()
    {
        using var system = ActorSystem.Create("filter-stream", ConfigurationFactory.ParseString(Named));
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(new SingleService(system));
        var a = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "a");
        var b = system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), "b");

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        var stream = plugin.InvokeStreamAsync("akka://embedded", BowireAkkaProtocol.TapServiceName,
            BowireAkkaProtocol.MonitorMethodName, ["""{ "paths": ["/user/b"] }"""], false, ct: cts.Token).GetAsyncEnumerator(cts.Token);
        try
        {
            var first = stream.MoveNextAsync();   // subscribes
            await Task.Delay(200, TestContext.Current.CancellationToken);
            a.Tell("not-this");
            b.Tell("this");
            Assert.True(await first);
            using var doc = JsonDocument.Parse(stream.Current);
            Assert.Equal("this", doc.RootElement.GetProperty("Payload").GetString());
        }
        finally { await stream.DisposeAsync(); }
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
#pragma warning restore CA1812
}
