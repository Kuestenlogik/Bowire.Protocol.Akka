// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json;
using Akka.Actor;
using Akka.Configuration;
using Kuestenlogik.Bowire.Models;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// Sending into the actor system from the workbench (#32).
/// </summary>
/// <remarks>
/// This is the one place the plugin stops being an observer, so most of what
/// is here is about what must not get through: a path the host did not
/// allow, a type it did not list, a selection that climbs out with <c>..</c>
/// or fans out with <c>*</c>, an address elsewhere, a <c>$type</c> in the
/// body. Off unless the host turns it on — the workbench cannot.
/// </remarks>
public sealed class TellTests
{
    private static readonly TellPolicy Docks = new(["/user/dock-*"], [typeof(PortCall)]);

    private static (ActorSystem System, BowireAkkaExtension Ext, BowireAkkaProtocol Plugin) Start(string name, string hocon = "")
    {
        var system = ActorSystem.Create(name, ConfigurationFactory.ParseString(hocon));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(new SingleService(system));
        return (system, ext, plugin);
    }

    private static string Request(string path, string type = "Kuestenlogik.Bowire.Protocol.Akka.Tests.PortCall", string message = """{ "shipId": 17, "name": "Nordstern" }""", int? replyTimeoutMs = null) =>
        $$"""{ "path": {{JsonSerializer.Serialize(path)}}, "messageType": {{JsonSerializer.Serialize(type)}}, "message": {{message}}{{(replyTimeoutMs is { } t ? $", \"replyTimeoutMs\": {t}" : "")}} }""";

    private static async Task<IBowireChannel> Open(BowireAkkaProtocol plugin) =>
        (await plugin.OpenChannelAsync("akka://embedded", BowireAkkaProtocol.TapServiceName, BowireAkkaProtocol.TellMethodName,
            false, ct: TestContext.Current.CancellationToken))!;

    private static async Task<string> NextResponse(IBowireChannel channel)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var r in channel.ReadResponsesAsync(cts.Token)) return r;
        throw new TimeoutException();
    }

    // ---- off by default ----

    [Fact]
    public async Task Without_The_Host_Allowing_It_There_Is_No_Tell_At_All()
    {
        var (system, _, plugin) = Start("tell-off");
        using var _s = system;
        var services = await plugin.DiscoverAsync("akka://embedded", false, TestContext.Current.CancellationToken);
        Assert.DoesNotContain(Assert.Single(services).Methods, m => m.Name == BowireAkkaProtocol.TellMethodName);
        Assert.Null(await plugin.OpenChannelAsync("akka://embedded", BowireAkkaProtocol.TapServiceName,
            BowireAkkaProtocol.TellMethodName, false, ct: TestContext.Current.CancellationToken));

        var result = await plugin.InvokeAsync("akka://embedded", BowireAkkaProtocol.TapServiceName,
            BowireAkkaProtocol.TellMethodName, [Request("/user/dock-1")], false, ct: TestContext.Current.CancellationToken);
        Assert.Equal("refused", result.Status);
    }

    [Fact]
    public async Task Once_Allowed_Discovery_Offers_The_Allowed_Types_To_Choose_From()
    {
        var (system, ext, plugin) = Start("tell-discover");
        using var _s = system;
        ext.EnableTell(Docks);
        var services = await plugin.DiscoverAsync("akka://embedded", false, TestContext.Current.CancellationToken);
        var tell = Assert.Single(services).Methods.Single(m => m.Name == BowireAkkaProtocol.TellMethodName);
        Assert.True(tell.ClientStreaming && tell.ServerStreaming);
        var types = tell.InputType.Fields.Single(f => f.Name == "messageType").EnumValues!;
        Assert.Equal([typeof(PortCall).FullName], types.Select(t => t.Name));
    }

    // ---- through the channel ----

    [Fact]
    public async Task An_Allowed_Tell_Arrives_Typed_And_The_Reply_Comes_Back()
    {
        var (system, ext, plugin) = Start("tell-ok");
        using var _s = system;
        ext.EnableTell(Docks);
        var received = new ConcurrentQueue<object>();
        system.ActorOf(Props.Create(() => new EchoActor(received)), "dock-1");
        await using var channel = await Open(plugin);

        Assert.True(await channel.SendAsync(Request("/user/dock-1"), TestContext.Current.CancellationToken));

        using var reply = JsonDocument.Parse(await NextResponse(channel));
        Assert.Equal("akka://tell-ok/user/dock-1", reply.RootElement.GetProperty("From").GetString());
        Assert.Contains("Nordstern", reply.RootElement.GetProperty("Payload").GetString(), StringComparison.Ordinal);
        Assert.Equal(new PortCall(17, "Nordstern"), Assert.Single(received));
        Assert.Equal(1, channel.SentCount);
    }

    [Theory]
    [InlineData("/user/harbor-master")]                  // not allowed
    [InlineData("/user/dock-1/../harbor-master")]        // climbs out of an allowed path
    [InlineData("/user/dock-*")]                         // would send to every dock
    [InlineData("/user/dock-?")]
    [InlineData("/user//dock-1")]
    [InlineData("user/dock-1")]                          // not from the root
    [InlineData("akka://other-system/user/dock-1")]      // another actor system
    [InlineData("akka.tcp://tell-refused@10.0.0.1:4053/user/dock-1")]  // over the network
    [InlineData("/user/dock-1#12345")]
    public async Task A_Path_The_Policy_Did_Not_Mean_Is_Refused(string path)
    {
        var (system, ext, plugin) = Start("tell-refused");
        using var _s = system;
        ext.EnableTell(Docks);
        var received = new ConcurrentQueue<object>();
        system.ActorOf(Props.Create(() => new EchoActor(received)), "dock-1");
        system.ActorOf(Props.Create(() => new EchoActor(received)), "harbor-master");
        await using var channel = await Open(plugin);

        Assert.False(await channel.SendAsync(Request(path), TestContext.Current.CancellationToken));
        using var refusal = JsonDocument.Parse(await NextResponse(channel));
        Assert.False(string.IsNullOrEmpty(refusal.RootElement.GetProperty("refused").GetString()));
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.Empty(received);
    }

    [Fact]
    public async Task The_Full_Local_Path_Is_Accepted_Too()
    {
        var (system, ext, plugin) = Start("tell-full");
        using var _s = system;
        ext.EnableTell(Docks);
        var received = new ConcurrentQueue<object>();
        system.ActorOf(Props.Create(() => new EchoActor(received)), "dock-2");
        await using var channel = await Open(plugin);
        Assert.True(await channel.SendAsync(Request("akka://tell-full/user/dock-2"), TestContext.Current.CancellationToken));
        await NextResponse(channel);
        Assert.Single(received);
    }

    [Theory]
    [InlineData("System.String")]
    [InlineData("Kuestenlogik.Bowire.Protocol.Akka.Tests.Binary")]
    [InlineData("")]
    public async Task A_Type_The_Host_Did_Not_List_Is_Refused(string type)
    {
        var (system, ext, plugin) = Start("tell-type");
        using var _s = system;
        ext.EnableTell(Docks);
        var received = new ConcurrentQueue<object>();
        system.ActorOf(Props.Create(() => new EchoActor(received)), "dock-1");
        await using var channel = await Open(plugin);

        Assert.False(await channel.SendAsync(Request("/user/dock-1", type, """{ "value": 1 }"""), TestContext.Current.CancellationToken));
        await NextResponse(channel);
        Assert.Empty(received);
    }

    [Fact]
    public async Task A_Type_Named_In_The_Body_Is_Ignored()
    {
        // The body is deserialized into the chosen, allowed type — a "$type"
        // naming something else changes nothing.
        var (system, ext, plugin) = Start("tell-smuggle");
        using var _s = system;
        ext.EnableTell(Docks);
        var received = new ConcurrentQueue<object>();
        system.ActorOf(Props.Create(() => new EchoActor(received)), "dock-1");
        await using var channel = await Open(plugin);

        const string body = """{ "$type": "System.Diagnostics.Process, System.Diagnostics.Process", "shipId": 3, "name": "x" }""";
        Assert.True(await channel.SendAsync(Request("/user/dock-1", message: body), TestContext.Current.CancellationToken));
        await NextResponse(channel);
        Assert.IsType<PortCall>(Assert.Single(received));
    }

    [Fact]
    public async Task A_Message_That_Does_Not_Fit_The_Type_Is_Refused_With_The_Reason()
    {
        var (system, ext, plugin) = Start("tell-shape");
        using var _s = system;
        ext.EnableTell(Docks);
        await using var channel = await Open(plugin);
        Assert.False(await channel.SendAsync(Request("/user/dock-1", message: """{ "shipId": "not a number" }"""), TestContext.Current.CancellationToken));
        using var refusal = JsonDocument.Parse(await NextResponse(channel));
        Assert.Contains("PortCall", refusal.RootElement.GetProperty("refused").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Taking_The_Permission_Back_Stops_An_Open_Channel()
    {
        var (system, ext, plugin) = Start("tell-revoke");
        using var _s = system;
        ext.EnableTell(Docks);
        var received = new ConcurrentQueue<object>();
        system.ActorOf(Props.Create(() => new EchoActor(received)), "dock-1");
        await using var channel = await Open(plugin);

        ext.DisableTell();
        Assert.False(await channel.SendAsync(Request("/user/dock-1"), TestContext.Current.CancellationToken));
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.Empty(received);
    }

    // ---- the invoke pane ----

    [Fact]
    public async Task One_Tell_From_The_Invoke_Pane_Can_Wait_For_Its_Reply()
    {
        var (system, ext, plugin) = Start("tell-invoke");
        using var _s = system;
        ext.EnableTell(Docks);
        system.ActorOf(Props.Create(() => new EchoActor(new ConcurrentQueue<object>())), "dock-1");

        var result = await plugin.InvokeAsync("akka://embedded", BowireAkkaProtocol.TapServiceName,
            BowireAkkaProtocol.TellMethodName, [Request("/user/dock-1", replyTimeoutMs: 3000)], false,
            ct: TestContext.Current.CancellationToken);

        Assert.Equal("OK", result.Status);
        using var doc = JsonDocument.Parse(result.Response!);
        Assert.StartsWith("ack:", doc.RootElement.GetProperty("reply").GetProperty("Payload").GetString(), StringComparison.Ordinal);
    }

    // ---- the policy ----

    [Fact]
    public void The_Host_Can_Allow_Tell_In_Hocon()
    {
        var hocon = $$"""
            bowire.akka.tell {
              paths = ["/user/dock-*"]
              message-types = ["{{typeof(PortCall).AssemblyQualifiedName}}", "No.Such.Type, Nowhere"]
            }
            """;
        var (system, ext, _) = Start("tell-hocon", hocon);
        using var _s = system;
        Assert.NotNull(ext.TellPolicy);
        Assert.Equal([typeof(PortCall).FullName], ext.TellPolicy!.MessageTypes.Keys);
    }

    [Fact]
    public void A_Hocon_Section_That_Allows_Nothing_Enables_Nothing()
    {
        var (system, ext, _) = Start("tell-hocon-empty", """bowire.akka.tell { paths = ["/user/x"], message-types = ["No.Such.Type, Nowhere"] }""");
        using var _s = system;
        Assert.Null(ext.TellPolicy);
    }

    [Fact]
    public void A_Policy_Needs_A_Path_And_A_Type()
    {
        Assert.Throws<ArgumentException>(() => new TellPolicy([], [typeof(PortCall)]));
        Assert.Throws<ArgumentException>(() => new TellPolicy(["/user/a"], []));
    }

    private sealed class SingleService(ActorSystem system) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(ActorSystem) ? system : null;
    }

#pragma warning disable CA1812 // instantiated by Akka via reflection
    private sealed class EchoActor(ConcurrentQueue<object> received) : UntypedActor
    {
        protected override void OnReceive(object message)
        {
            received.Enqueue(message);
            Sender.Tell($"ack:{message}");
        }
    }
#pragma warning restore CA1812
}
