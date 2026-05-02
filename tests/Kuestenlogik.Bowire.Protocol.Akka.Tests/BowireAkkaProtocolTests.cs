// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Akka.Actor;
using Akka.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

public sealed class BowireAkkaProtocolTests
{
    [Fact]
    public void Identity_MatchesBowireConventions()
    {
        var plugin = new BowireAkkaProtocol();
        Assert.Equal("akka", plugin.Id);
        Assert.Equal("Akka.NET", plugin.Name);
        Assert.Contains("svg", plugin.IconSvg, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Discover_WithoutActorSystem_ReturnsEmpty()
    {
        // No DI / no Initialize → standalone mode placeholder; nothing
        // to surface yet.
        var plugin = new BowireAkkaProtocol();
        var services = await plugin.DiscoverAsync(
            "akka://embedded", false, TestContext.Current.CancellationToken);
        Assert.Empty(services);
    }

    [Fact]
    public async Task Discover_WithActorSystem_ReturnsTapService()
    {
        using var system = ActorSystem.Create("discovery-test");
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(BuildSp(system));

        var services = await plugin.DiscoverAsync(
            "akka://embedded", false, TestContext.Current.CancellationToken);

        var tap = Assert.Single(services);
        Assert.Equal(BowireAkkaProtocol.TapServiceName, tap.Name);
        var monitor = Assert.Single(tap.Methods);
        Assert.Equal(BowireAkkaProtocol.MonitorMethodName, monitor.Name);
        Assert.True(monitor.ServerStreaming);
    }

    [Fact]
    public async Task InvokeStream_YieldsTappedMessageAsJson()
    {
        const string hocon = """
            akka.actor.bowire-tap = {
              mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
            }
            """;
        using var system = ActorSystem.Create("invoke-stream-test", ConfigurationFactory.ParseString(hocon));
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(BuildSp(system));

        // Drive the streaming subscription on a background task so we can
        // produce a message AFTER the subscriber registers — otherwise
        // the tap mailbox short-circuits (no subscribers).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var stream = plugin.InvokeStreamAsync(
            "akka://embedded",
            BowireAkkaProtocol.TapServiceName,
            BowireAkkaProtocol.MonitorMethodName,
            [], false, null, cts.Token);

        var pump = Task.Run(async () =>
        {
            // Heartbeat — re-Tell every 100 ms until the consumer has
            // pulled the first envelope and signalled cancellation.
            // Initial Subscribe→ActorOf race is what this works around.
            for (var i = 0; i < 40 && !cts.IsCancellationRequested; i++)
            {
                if (system.WhenTerminated.IsCompleted) break;
                var ping = system.ActorOf(PingActor.Build().WithMailbox("akka.actor.bowire-tap"), $"ping-{i}");
                ping.Tell("hello");
                await Task.Delay(100, cts.Token);
            }
        }, TestContext.Current.CancellationToken);

        try
        {
            await foreach (var json in stream.WithCancellation(cts.Token))
            {
                using var doc = JsonDocument.Parse(json);
                Assert.Equal("System.String", doc.RootElement.GetProperty("MessageType").GetString());
                Assert.Equal("hello", doc.RootElement.GetProperty("Payload").GetString());
                await cts.CancelAsync();
                break;
            }
        }
        catch (OperationCanceledException) { /* expected after first yield */ }
        try { await pump; } catch (OperationCanceledException) { /* expected */ }
    }

    private static ServiceProvider BuildSp(ActorSystem system)
    {
        var services = new ServiceCollection();
        services.AddSingleton(system);
        return services.BuildServiceProvider();
    }

#pragma warning disable CA1812 // Akka instantiates via reflection in Props.Create<T>()
    private sealed class PingActor : UntypedActor
    {
        public static Props Build() => global::Akka.Actor.Props.Create<PingActor>();
        protected override void OnReceive(object message) { /* no-op */ }
    }
#pragma warning restore CA1812
}
