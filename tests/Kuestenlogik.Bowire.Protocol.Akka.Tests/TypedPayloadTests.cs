// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Configuration;
using Akka.Serialization;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// Messages as structured JSON on the tap, in the format the user picks (#30).
/// </summary>
/// <remarks>
/// Rendering runs inside the sender's Tell, so it happens only for a reader
/// that asked, once per message and format however many asked — the counting
/// serializer below holds that — and a message that will not render in the
/// chosen format is still delivered, with its string rendering.
/// </remarks>
public sealed class TypedPayloadTests
{
    private static readonly string Asm = typeof(TypedPayloadTests).Assembly.GetName().Name!;

    private static string Hocon(bool withBindings) => $$"""
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        {{(withBindings ? $$"""
        akka.actor.serializers.notjson = "{{typeof(NotJsonSerializer).FullName}}, {{Asm}}"
        akka.actor.serializers.counting = "{{typeof(CountingJsonSerializer).FullName}}, {{Asm}}"
        akka.actor.serialization-bindings {
          "{{typeof(Binary).FullName}}, {{Asm}}" = notjson
          "{{typeof(Custom).FullName}}, {{Asm}}" = counting
        }
        """ : "")}}
        """;

    private static IActorRef Tapped(ActorSystem system, string name) =>
        system.ActorOf(Props.Create<SinkActor>().WithMailbox("akka.actor.bowire-tap"), name);

    private static async Task<TappedMessage> Next(ChannelReader<TappedMessage> reader, string typeSuffix, bool deadLetter = false)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            var m = await reader.ReadAsync(cts.Token);
            if (m.MessageType.EndsWith(typeSuffix, StringComparison.Ordinal) && m.IsDeadLetter == deadLetter) return m;
        }
    }

    /// <summary>Send <paramref name="message"/> and read it back rendered as <paramref name="format"/>.</summary>
    private static async Task<JsonElement?> Rendered(object message, PayloadFormat format, bool withBindings = false)
    {
        using var system = ActorSystem.Create("typed", ConfigurationFactory.ParseString(Hocon(withBindings)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, format, out var token);
        try
        {
            Tapped(system, "sink").Tell(message);
            return (await Next(reader, message.GetType().Name)).PayloadJson;
        }
        finally { ext.Unsubscribe(token); }
    }

    // ---- auto ----

    [Fact]
    public async Task Auto_Shows_A_Record_With_Numbers_As_Numbers()
    {
        // Not Akka's wire JSON, where 17 is {"$": "I17"}.
        var json = (await Rendered(new PortCall(17, "Nordstern"), PayloadFormat.Auto))!.Value;
        Assert.Equal(17, json.GetProperty("ShipId").GetInt32());
        Assert.Equal("Nordstern", json.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Auto_Shows_A_Json_Serializer_Bound_On_Purpose()
    {
        var json = (await Rendered(new Custom(5), PayloadFormat.Auto, withBindings: true))!.Value;
        Assert.Equal("custom-format", json.GetProperty("format").GetString());
    }

    [Fact]
    public async Task Auto_Shows_A_Protobuf_Message_In_Protobufs_Own_Json()
    {
        // The canonical mapping of a Duration is a string; its properties
        // would be {"Seconds": 1, "Nanos": 500000000}.
        var json = (await Rendered(new Google.Protobuf.WellKnownTypes.Duration { Seconds = 1, Nanos = 500_000_000 }, PayloadFormat.Auto))!.Value;
        Assert.Equal(JsonValueKind.String, json.ValueKind);
        Assert.Equal("1.500s", json.GetString());
    }

    [Fact]
    public async Task Auto_Falls_Back_To_The_Fields_When_The_Properties_Show_Nothing()
    {
        var json = (await Rendered(new PrivateOnly(5), PayloadFormat.Auto))!.Value;
        Assert.Equal(5, json.GetProperty("_value").GetInt32());
    }

    [Fact]
    public async Task Auto_Gets_Through_A_Binary_Binding_By_The_Properties()
    {
        var json = (await Rendered(new Binary(42), PayloadFormat.Auto, withBindings: true))!.Value;
        Assert.Equal(42, json.GetProperty("Value").GetInt32());
    }

    // ---- the explicit formats ----

    [Fact]
    public async Task Properties_Is_Only_The_Public_Properties()
    {
        var json = (await Rendered(new PrivateOnly(5), PayloadFormat.Properties))!.Value;
        Assert.Empty(json.EnumerateObject());
    }

    [Fact]
    public async Task Fields_Shows_The_State_With_Auto_Property_Names_Cleaned_Up()
    {
        var record = (await Rendered(new PortCall(17, "N"), PayloadFormat.Fields))!.Value;
        Assert.Equal(17, record.GetProperty("ShipId").GetInt32());   // not "<ShipId>k__BackingField"

        var hidden = (await Rendered(new PrivateOnly(3), PayloadFormat.Fields))!.Value;
        Assert.Equal(3, hidden.GetProperty("_value").GetInt32());
    }

    [Fact]
    public async Task Fields_Survives_A_Cycle()
    {
        var node = new Node("a");
        node.Next = node;
        var json = (await Rendered(node, PayloadFormat.Fields))!.Value;
        Assert.Equal("<cycle>", json.GetProperty("Next").GetString());
    }

    [Fact]
    public async Task Akka_Shows_The_Default_Serializers_Wire_Json()
    {
        var json = (await Rendered(new PortCall(17, "N"), PayloadFormat.Akka))!.Value;
        Assert.True(json.TryGetProperty("$type", out _));
        Assert.Equal("I17", json.GetProperty("ShipId").GetProperty("$").GetString());
    }

    [Fact]
    public async Task Akka_Shows_A_Binary_Serializers_Bytes_With_Its_Name()
    {
        // What a protobuf or Hyperion binding would put on the wire.
        var json = (await Rendered(new Binary(42), PayloadFormat.Akka, withBindings: true))!.Value;
        Assert.Equal(typeof(NotJsonSerializer).FullName, json.GetProperty("serializer").GetString());
        Assert.Equal(3, json.GetProperty("bytes").GetInt32());
        Assert.Equal("/wAB", json.GetProperty("base64").GetString());
    }

    // ---- cost and failure ----

    [Fact]
    public async Task Rendered_Only_When_Asked_And_Once_Per_Format_However_Many_Ask()
    {
        using var system = ActorSystem.Create("typed-count", ConfigurationFactory.ParseString(Hocon(true)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var counting = (CountingJsonSerializer)system.Serialization.FindSerializerForType(typeof(Custom));
        var sink = Tapped(system, "sink");

        var plain = ext.Subscribe(out var t0);
        sink.Tell(new Custom(1));
        Assert.Null((await Next(plain, "Custom")).PayloadJson);
        Assert.Equal(0, counting.Calls);

        var a = ext.Subscribe(null, PayloadFormat.Auto, out var t1);
        var b = ext.Subscribe(null, PayloadFormat.Auto, out var t2);
        var c = ext.Subscribe(null, PayloadFormat.Properties, out var t3);
        try
        {
            sink.Tell(new Custom(2));
            Assert.NotNull((await Next(a, "Custom")).PayloadJson);
            Assert.NotNull((await Next(b, "Custom")).PayloadJson);
            Assert.NotNull((await Next(c, "Custom")).PayloadJson);
            // Two auto readers, one rendering; the properties reader never asks the serializer.
            Assert.Equal(1, counting.Calls);
        }
        finally
        {
            ext.Unsubscribe(t0); ext.Unsubscribe(t1); ext.Unsubscribe(t2); ext.Unsubscribe(t3);
        }
    }

    [Fact]
    public async Task A_Message_That_Will_Not_Render_Is_Still_Delivered()
    {
        using var system = ActorSystem.Create("typed-broken", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, PayloadFormat.Properties, out var token);
        try
        {
            var probe = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            var actor = system.ActorOf(Props.Create(() => new ProbeActor(probe)).WithMailbox("akka.actor.bowire-tap"), "probe");
            actor.Tell(new Unserializable());

            Assert.Null((await Next(reader, "Unserializable")).PayloadJson);
            Assert.IsType<Unserializable>(await probe.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task Where_The_Properties_Throw_Auto_Still_Shows_The_Fields()
    {
        var json = (await Rendered(new Unserializable(), PayloadFormat.Auto))!.Value;
        Assert.Equal(0, json.GetProperty("value").GetInt32());
    }

    [Fact]
    public async Task A_Payload_Too_Large_To_Show_Is_Left_Out()
    {
        Assert.Null(await Rendered(new PortCall(1, new string('x', PayloadRenderer.MaxBytes + 1)), PayloadFormat.Auto));
    }

    [Fact]
    public async Task A_Dead_Letter_Is_Rendered_Too()
    {
        using var system = ActorSystem.Create("typed-dead", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, PayloadFormat.Auto, out var token);
        try
        {
            var doomed = Tapped(system, "doomed");
            await doomed.GracefulStop(TimeSpan.FromSeconds(3));
            doomed.Tell(new PortCall(9, "Late"));
            var tap = await Next(reader, "PortCall", deadLetter: true);
            Assert.Equal(9, tap.PayloadJson!.Value.GetProperty("ShipId").GetInt32());
        }
        finally { ext.Unsubscribe(token); }
    }

    // ---- the request ----

    [Theory]
    [InlineData("""{ "payloadFormat": "auto" }""", PayloadFormat.Auto)]
    [InlineData("""{ "payloadFormat": "FIELDS" }""", PayloadFormat.Fields)]
    [InlineData("""{ "payloadFormat": 4 }""", PayloadFormat.Akka)]
    [InlineData("""{ "payloadFormat": "3" }""", PayloadFormat.None)]
    [InlineData("""{ "payloadFormat": 99 }""", PayloadFormat.None)]
    [InlineData("""{ "payloadFormat": "pretty" }""", PayloadFormat.None)]
    [InlineData("{}", PayloadFormat.None)]
    [InlineData("nope", PayloadFormat.None)]
    public void The_Request_Picks_A_Format_By_Name_Or_Number(string body, PayloadFormat expected)
    {
        Assert.Equal(expected, BowireAkkaProtocol.ReadPayloadFormat([body]));
    }

    [Fact]
    public async Task Discovery_Offers_The_Formats_To_Choose_From()
    {
        using var system = ActorSystem.Create("typed-discover");
        var plugin = new BowireAkkaProtocol();
        plugin.Initialize(new SingleService(system));
        var services = await plugin.DiscoverAsync("akka://embedded", false, TestContext.Current.CancellationToken);
        var field = Assert.Single(services).Methods.Single(m => m.Name == BowireAkkaProtocol.MonitorMethodName)
            .InputType.Fields.Single(f => f.Name == "payloadFormat");
        Assert.Equal("enum", field.Type);
        Assert.Equal(["none", "auto", "properties", "fields", "akka"], field.EnumValues!.Select(v => v.Name));
    }

    [Fact]
    public void Serialized_To_The_Stream_The_Structured_Payload_Is_Json_Not_A_String()
    {
        using var doc = JsonDocument.Parse("""{ "a": 1 }""");
        var tap = new TappedMessage("r", "s", "T", "p", DateTime.UtcNow, PayloadJson: doc.RootElement.Clone());
        using var wire = JsonDocument.Parse(JsonSerializer.Serialize(tap));
        Assert.Equal(1, wire.RootElement.GetProperty("PayloadJson").GetProperty("a").GetInt32());
    }

    private sealed class SingleService(ActorSystem system) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(ActorSystem) ? system : null;
    }

#pragma warning disable CA1812 // instantiated by Akka via reflection
    private sealed class SinkActor : UntypedActor
    {
        protected override void OnReceive(object message) { }
    }

    private sealed class ProbeActor(TaskCompletionSource<object> probe) : UntypedActor
    {
        protected override void OnReceive(object message) => probe.TrySetResult(message);
    }
#pragma warning restore CA1812
}

internal sealed record PortCall(int ShipId, string Name);

internal sealed record Binary(int Value);

internal sealed record Custom(int Value);

/// <summary>State only in a private field, as Hyperion users often write it.</summary>
internal sealed class PrivateOnly(int value)
{
    private readonly int _value = value;
    public override string ToString() => $"PrivateOnly({_value})";
}

internal sealed class Node(string name)
{
    public string Name { get; } = name;
    public Node? Next { get; set; }
}

/// <summary>A property that cannot be read.</summary>
internal sealed class Unserializable(int value = 0)
{
    public int Boom => value == 0 ? throw new InvalidOperationException("no") : value;
}

#pragma warning disable CA1812 // instantiated by Akka via reflection
/// <summary>A serializer whose output is not JSON, as a protobuf or Hyperion binding would be.</summary>
internal sealed class NotJsonSerializer(ExtendedActorSystem system) : Serializer(system)
{
    public override int Identifier => 947_011;
    public override bool IncludeManifest => false;
    public override byte[] ToBinary(object obj) => [0xFF, 0x00, 0x01];
    public override object FromBinary(byte[] bytes, Type type) => new Binary(0);
}

/// <summary>An application's own JSON format for a type, counting its calls.</summary>
internal sealed class CountingJsonSerializer(ExtendedActorSystem system) : Serializer(system)
{
    private int _calls;
    public int Calls => Volatile.Read(ref _calls);
    public override int Identifier => 947_012;
    public override bool IncludeManifest => false;
    public override byte[] ToBinary(object obj)
    {
        Interlocked.Increment(ref _calls);
        return """{ "format": "custom-format" }"""u8.ToArray();
    }
    public override object FromBinary(byte[] bytes, Type type) => new Custom(0);
}
#pragma warning restore CA1812
