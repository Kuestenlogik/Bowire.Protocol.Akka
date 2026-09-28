// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.Tools.Client;
using Akka.Configuration;

namespace Kuestenlogik.Bowire.Protocol.Akka.Remote.Tests;

/// <summary>
/// The standalone CLI streaming a running cluster's taps over ClusterClient
/// (#27) — a real one-node cluster and the CLI's own actor system, in one
/// process, over loopback remoting.
/// </summary>
public sealed class RemoteTapTests : IAsyncLifetime
{
    private const string TapMailbox = """
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        """;

    private readonly List<ActorSystem> _hosts = [];

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await BowireAkkaRemoteProtocol.ShutdownAllAsync();
        foreach (var host in _hosts) await host.Terminate();
    }

    /// <summary>A one-node cluster on a free loopback port, up.</summary>
    private async Task<(ActorSystem System, string Url)> Host(string name, string extra = "")
    {
        var config = ConfigurationFactory.ParseString($$"""
            akka.actor.provider = cluster
            akka.remote.dot-netty.tcp { hostname = "127.0.0.1", port = 0 }
            akka.loglevel = WARNING
            {{TapMailbox}}
            {{extra}}
            """).WithFallback(ClusterClientReceptionist.DefaultConfig());
        var system = ActorSystem.Create(name, config);
        _hosts.Add(system);
        var cluster = Cluster.Get(system);
        var up = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.RegisterOnMemberUp(() => up.TrySetResult());
        await cluster.JoinAsync(cluster.SelfAddress, TestContext.Current.CancellationToken);
        await up.Task.WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);
        return (system, cluster.SelfAddress.ToString());
    }

    private static IActorRef Tapped(ActorSystem system, string name) =>
        system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), name);

    // ---- the URL ----

    [Theory]
    [InlineData("akka.tcp://Harbor@10.0.0.5:4053", "akka.tcp://Harbor@10.0.0.5:4053", "Harbor", "localhost")]
    [InlineData("akka.tcp://Harbor@10.0.0.5:4053/?clientHost=10.0.0.9", "akka.tcp://Harbor@10.0.0.5:4053", "Harbor", "10.0.0.9")]
    [InlineData("akka://Harbor@host:4053", "akka.tcp://Harbor@host:4053", "Harbor", "localhost")]
    public void A_Contact_Point_Is_Read_From_The_Url(string text, string address, string system, string clientHost)
    {
        Assert.Equal(new BowireAkkaRemoteProtocol.Contact(address, system, clientHost), BowireAkkaRemoteProtocol.Contact.TryParse(text));
    }

    [Theory]
    [InlineData("akka://embedded")]          // the embedded plugin's URL: no host, not ours
    [InlineData("http://localhost:5000")]
    [InlineData("")]
    public void Anything_Else_Is_Not_A_Contact_Point(string text)
    {
        Assert.Null(BowireAkkaRemoteProtocol.Contact.TryParse(text));
    }

    // ---- discovery ----

    [Fact]
    public async Task Without_The_Relay_Discovery_Says_What_Is_Missing()
    {
        var (_, url) = await Host("no-relay");
        var plugin = new BowireAkkaRemoteProtocol();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            plugin.DiscoverAsync(url, false, TestContext.Current.CancellationToken));
        Assert.Contains("EnableBowireRemoteTap", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task With_The_Relay_Discovery_Offers_The_Tap_Stream()
    {
        var (system, url) = await Host("with-relay");
        system.EnableBowireRemoteTap();
        var plugin = new BowireAkkaRemoteProtocol();
        var services = await plugin.DiscoverAsync(url, false, TestContext.Current.CancellationToken);
        var method = Assert.Single(Assert.Single(services).Methods);
        Assert.Equal(BowireAkkaProtocol.MonitorMethodName, method.Name);
        Assert.True(method.ServerStreaming);
    }

    // ---- the stream ----

    [Fact]
    public async Task The_Cli_Streams_The_Hosts_Taps_Filtered_And_Rendered_On_The_Host()
    {
        var (system, url) = await Host("stream");
        system.EnableBowireRemoteTap();
        var dock = Tapped(system, "dock-1");
        var master = Tapped(system, "harbor-master");
        var plugin = new BowireAkkaRemoteProtocol();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        var stream = plugin.InvokeStreamAsync(url, BowireAkkaProtocol.TapServiceName, BowireAkkaProtocol.MonitorMethodName,
            ["""{ "paths": ["/user/dock-*"], "payloadFormat": "auto" }"""], false, ct: cts.Token).GetAsyncEnumerator(cts.Token);
        try
        {
            var first = stream.MoveNextAsync();
            // Keep telling until the subscription is in place on the host.
            var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
            while (!ext.HasSubscribers) await Task.Delay(50, cts.Token);
            master.Tell(new ShipDocked(1, "not-this"));
            dock.Tell(new ShipDocked(17, "Nordstern"));

            Assert.True(await first);
            using var doc = JsonDocument.Parse(stream.Current);
            Assert.Equal(dock.Path.ToString(), doc.RootElement.GetProperty("Recipient").GetString());
            Assert.Equal(17, doc.RootElement.GetProperty("PayloadJson").GetProperty("ShipId").GetInt32());
        }
        finally { await stream.DisposeAsync(); }
    }

    [Fact]
    public async Task Ending_The_Stream_Ends_The_Subscription_On_The_Host()
    {
        var (system, url) = await Host("unsubscribe");
        system.EnableBowireRemoteTap();
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var plugin = new BowireAkkaRemoteProtocol();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var stream = plugin.InvokeStreamAsync(url, BowireAkkaProtocol.TapServiceName, BowireAkkaProtocol.MonitorMethodName,
            [], false, ct: cts.Token).GetAsyncEnumerator(cts.Token);
        var pending = stream.MoveNextAsync().AsTask();
        await WaitFor(() => ext.HasSubscribers);

        await cts.CancelAsync();
        try { await pending; } catch (OperationCanceledException) { /* what cancelling is */ }
        await stream.DisposeAsync();
        await WaitFor(() => !ext.HasSubscribers);
    }

    [Fact]
    public async Task A_Cli_That_Went_Away_Stops_Being_Served_When_Its_Lease_Runs_Out()
    {
        // No renewal and no unsubscribe: a probe subscribes once through the
        // relay and is never heard of again.
        var (system, _) = await Host("lease", "bowire.akka.remote-tap { lease = 1s, heartbeat = 500ms }");
        var relay = system.EnableBowireRemoteTap();
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var probe = system.ActorOf(Props.Create<SinkActor>());

        relay.Tell(new RemoteTapSubscribe("gone", [], [], true, 0), probe);
        await WaitFor(() => ext.HasSubscribers);
        await WaitFor(() => !ext.HasSubscribers, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Enabling_Twice_Is_The_Same_Relay()
    {
        var (system, _) = await Host("twice");
        Assert.Equal(system.EnableBowireRemoteTap(), system.EnableBowireRemoteTap());
    }

    [Fact]
    public async Task The_Remote_Tap_Never_Sends_Into_The_Cluster()
    {
        var plugin = new BowireAkkaRemoteProtocol();
        Assert.Null(await plugin.OpenChannelAsync("akka.tcp://x@127.0.0.1:1", "Tap", "Tell", false,
            ct: TestContext.Current.CancellationToken));
    }

    private static async Task WaitFor(Func<bool> condition, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(10));
        while (!condition())
        {
            if (cts.IsCancellationRequested) Assert.Fail("condition not reached in time");
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }

#pragma warning disable CA1812 // instantiated by Akka via reflection
    private sealed class SinkActor : UntypedActor
    {
        protected override void OnReceive(object message) { }
    }
#pragma warning restore CA1812
}

internal sealed record ShipDocked(int ShipId, string Name);
