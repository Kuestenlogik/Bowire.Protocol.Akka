// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Akka.Actor;
using Akka.Configuration;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// Messages per second per actor (#29).
/// </summary>
/// <remarks>
/// The count is taken at the tap, not from the tap stream. The stream is a
/// bounded channel that drops the oldest entries when its reader falls
/// behind; a throughput figure read off it would be lowest exactly when the
/// actor is busiest. The first extension test holds that line.
/// </remarks>
public sealed class ThroughputTests
{
    private const string Named = """
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        """;

    private static IActorRef Tapped(ActorSystem system, string name) =>
        system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), name);

    // ---- the counter ----

    [Fact]
    public async Task No_Increment_Is_Lost_Or_Counted_Twice_While_Draining()
    {
        var counter = new ThroughputCounter();
        const int threads = 8, perThread = 50_000;
        long drained = 0;

        var writers = Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            for (var i = 0; i < perThread; i++) counter.Record(t % 2 == 0 ? "a" : "b");
        }, TestContext.Current.CancellationToken)).ToArray();

        var all = Task.WhenAll(writers);
        while (!all.IsCompleted)
        {
            drained += counter.Drain().Sum(c => c.Messages);
            await Task.Yield();
        }
        await all;
        drained += counter.Drain().Sum(c => c.Messages);

        Assert.Equal((long)threads * perThread, drained);
    }

    [Fact]
    public void The_Total_Runs_On_While_Each_Drain_Reports_Its_Own_Interval()
    {
        var counter = new ThroughputCounter();
        counter.Record("x"); counter.Record("x");
        Assert.Equal(("x", 2L, 2L), Assert.Single(counter.Drain()));
        counter.Record("x");
        Assert.Equal(("x", 1L, 3L), Assert.Single(counter.Drain()));
        Assert.Empty(counter.Drain());
    }

    // ---- at the tap ----

    [Fact]
    public async Task Every_Message_Is_Counted_Even_When_The_Stream_Reader_Falls_Behind()
    {
        using var system = ActorSystem.Create("tp-load", ConfigurationFactory.ParseString(Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        // A reader that never reads: its channel holds 1024 and drops the rest.
        ext.Subscribe(out var token);
        var counter = ext.StartCounting();
        try
        {
            var sink = Tapped(system, "sink");
            const int sent = 5000;
            for (var i = 0; i < sent; i++) sink.Tell(i);

            long counted = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (counted < sent && !cts.IsCancellationRequested)
            {
                counted += counter.Drain().Where(c => c.Path == sink.Path.ToString()).Sum(c => c.Messages);
                await Task.Delay(20, TestContext.Current.CancellationToken);
            }
            Assert.Equal(sent, counted);
        }
        finally
        {
            ext.StopCounting(counter);
            ext.Unsubscribe(token);
        }
    }

    [Fact]
    public void Counting_Alone_Switches_The_Tap_On_And_Stopping_Switches_It_Off()
    {
        using var system = ActorSystem.Create("tp-onoff", ConfigurationFactory.ParseString(Named));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        Assert.False(ext.HasSubscribers);
        var counter = ext.StartCounting();
        Assert.True(ext.HasSubscribers);
        ext.StopCounting(counter);
        Assert.False(ext.HasSubscribers);
    }

    // ---- the stream ----

    [Fact]
    public async Task The_Throughput_Stream_Names_The_Busy_Actor_With_Its_Count()
    {
        using var system = ActorSystem.Create("tp-stream", ConfigurationFactory.ParseString(Named));
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(new SingleService(system));
        var busy = Tapped(system, "busy");
        var quiet = Tapped(system, "quiet");

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        var seenBusy = 0L;
        var seenQuiet = 0L;
        var sent = false;
        await foreach (var json in plugin.InvokeStreamAsync(
            "akka://embedded", BowireAkkaProtocol.TapServiceName, BowireAkkaProtocol.ThroughputMethodName,
            ["""{ "intervalMs": 200 }"""], false, ct: cts.Token))
        {
            if (!sent)
            {
                // The first tick is the counter being live; send after it.
                for (var i = 0; i < 30; i++) busy.Tell(i);
                quiet.Tell("one");
                sent = true;
                continue;
            }
            using var doc = JsonDocument.Parse(json);
            foreach (var a in doc.RootElement.GetProperty("Actors").EnumerateArray())
            {
                var path = a.GetProperty("Path").GetString();
                if (path == busy.Path.ToString()) seenBusy = a.GetProperty("Total").GetInt64();
                if (path == quiet.Path.ToString()) seenQuiet = a.GetProperty("Total").GetInt64();
            }
            if (seenBusy == 30 && seenQuiet == 1) break;
        }

        Assert.Equal(30, seenBusy);
        Assert.Equal(1, seenQuiet);
    }

    [Theory]
    [InlineData(null, 1000, 50)]
    [InlineData("{}", 1000, 50)]
    [InlineData("""{ "intervalMs": 5, "top": 3 }""", 100, 3)]
    [InlineData("""{ "intervalMs": 999999, "top": 0 }""", 60000, 0)]
    [InlineData("""{ "top": -1 }""", 1000, 50)]
    [InlineData("not json", 1000, 50)]
    public void A_Request_Out_Of_Range_Falls_Back_Rather_Than_Failing(string? body, int intervalMs, int top)
    {
        var (interval, t) = BowireAkkaProtocol.ReadThroughputRequest(body is null ? [] : [body]);
        Assert.Equal(TimeSpan.FromMilliseconds(intervalMs), interval);
        Assert.Equal(top, t);
    }

    [Fact]
    public async Task Discovery_Lists_The_Throughput_Method_As_A_Server_Stream()
    {
        using var system = ActorSystem.Create("tp-discover");
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(new SingleService(system));
        var services = await plugin.DiscoverAsync("akka://embedded", false, TestContext.Current.CancellationToken);
        var method = Assert.Single(services).Methods.Single(m => m.Name == BowireAkkaProtocol.ThroughputMethodName);
        Assert.True(method.ServerStreaming);
        Assert.Contains(method.InputType.Fields, f => f.Name == "intervalMs");
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
