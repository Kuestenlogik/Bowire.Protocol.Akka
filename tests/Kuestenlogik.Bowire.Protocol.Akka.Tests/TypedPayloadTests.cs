// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Configuration;
using Akka.Serialization;

namespace Kuestenlogik.Bowire.Protocol.Akka.Tests;

/// <summary>
/// Messages as structured JSON on the tap (#30).
/// </summary>
/// <remarks>
/// Serializing runs inside the sender's Tell, so it happens only for a reader
/// that asked, once per message however many asked — the counting serializer
/// below holds both — and a message that will not serialize still gets
/// delivered, with its string rendering and no structured one.
/// </remarks>
public sealed class TypedPayloadTests
{
    private static readonly string Asm = typeof(TypedPayloadTests).Assembly.GetName().Name!;

    private static string Hocon(bool withCounting) => $$"""
        akka.actor.bowire-tap = {
          mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
        }
        {{(withCounting ? $$"""
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

    private static async Task<TappedMessage> Next(ChannelReader<TappedMessage> reader, Func<TappedMessage, bool> match)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            var m = await reader.ReadAsync(cts.Token);
            if (match(m)) return m;
        }
    }

    [Fact]
    public async Task A_Record_Arrives_With_Its_Fields_Numbers_As_Numbers()
    {
        using var system = ActorSystem.Create("typed-json", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, typedPayload: true, out var token);
        try
        {
            Tapped(system, "dock").Tell(new PortCall(17, "Nordstern"));
            var tap = await Next(reader, m => m.MessageType.EndsWith("PortCall", StringComparison.Ordinal));

            var json = tap.PayloadJson!.Value;
            // Not Akka's wire JSON, where this is {"$": "I17"}.
            Assert.Equal(17, json.GetProperty("ShipId").GetInt32());
            Assert.Equal("Nordstern", json.GetProperty("Name").GetString());
            // The string rendering stays: it is what a reader without the flag gets.
            Assert.Contains("Nordstern", tap.Payload, StringComparison.Ordinal);
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task A_Reader_That_Did_Not_Ask_Gets_No_Structured_Payload()
    {
        using var system = ActorSystem.Create("typed-off", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var plain = ext.Subscribe(out var t1);
        var typed = ext.Subscribe(null, typedPayload: true, out var t2);
        try
        {
            Tapped(system, "dock").Tell(new PortCall(1, "A"));
            Assert.Null((await Next(plain, m => m.Payload.Contains("PortCall", StringComparison.Ordinal))).PayloadJson);
            Assert.NotNull((await Next(typed, m => m.Payload.Contains("PortCall", StringComparison.Ordinal))).PayloadJson);
        }
        finally
        {
            ext.Unsubscribe(t1);
            ext.Unsubscribe(t2);
        }
    }

    [Fact]
    public async Task A_Binary_Serializer_Falls_Back_To_The_Public_Properties()
    {
        using var system = ActorSystem.Create("typed-binary", ConfigurationFactory.ParseString(Hocon(true)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, typedPayload: true, out var token);
        try
        {
            Tapped(system, "sink").Tell(new Binary(42));
            var tap = await Next(reader, m => m.MessageType.EndsWith("Binary", StringComparison.Ordinal));
            Assert.Equal(42, tap.PayloadJson!.Value.GetProperty("Value").GetInt32());
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task A_Json_Serializer_Bound_On_Purpose_Is_What_Is_Shown()
    {
        using var system = ActorSystem.Create("typed-custom", ConfigurationFactory.ParseString(Hocon(true)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, typedPayload: true, out var token);
        try
        {
            Tapped(system, "sink").Tell(new Custom(5));
            var tap = await Next(reader, m => m.MessageType.EndsWith("Custom", StringComparison.Ordinal));
            Assert.Equal("custom-format", tap.PayloadJson!.Value.GetProperty("format").GetString());
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task Serialized_Only_When_Asked_And_Once_However_Many_Ask()
    {
        using var system = ActorSystem.Create("typed-count", ConfigurationFactory.ParseString(Hocon(true)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var counting = (CountingJsonSerializer)system.Serialization.FindSerializerForType(typeof(Custom));
        var sink = Tapped(system, "sink");

        var plain = ext.Subscribe(out var t0);
        sink.Tell(new Custom(1));
        await Next(plain, m => m.MessageType.EndsWith("Custom", StringComparison.Ordinal));
        Assert.Equal(0, counting.Calls);

        var a = ext.Subscribe(null, typedPayload: true, out var t1);
        var b = ext.Subscribe(null, typedPayload: true, out var t2);
        try
        {
            sink.Tell(new Custom(2));
            await Next(a, m => m.MessageType.EndsWith("Custom", StringComparison.Ordinal));
            await Next(b, m => m.MessageType.EndsWith("Custom", StringComparison.Ordinal));
            Assert.Equal(1, counting.Calls);
        }
        finally
        {
            ext.Unsubscribe(t0);
            ext.Unsubscribe(t1);
            ext.Unsubscribe(t2);
        }
    }

    [Fact]
    public async Task A_Message_That_Will_Not_Serialize_Is_Still_Delivered()
    {
        using var system = ActorSystem.Create("typed-broken", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, typedPayload: true, out var token);
        try
        {
            var probe = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            var actor = system.ActorOf(Props.Create(() => new ProbeActor(probe)).WithMailbox("akka.actor.bowire-tap"), "probe");
            actor.Tell(new Unserializable());

            var tap = await Next(reader, m => m.MessageType.EndsWith("Unserializable", StringComparison.Ordinal));
            Assert.Null(tap.PayloadJson);
            Assert.IsType<Unserializable>(await probe.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task A_Payload_Too_Large_To_Show_Is_Left_Out()
    {
        using var system = ActorSystem.Create("typed-large", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, typedPayload: true, out var token);
        try
        {
            Tapped(system, "sink").Tell(new PortCall(1, new string('x', PayloadRenderer.MaxBytes + 1)));
            Assert.Null((await Next(reader, m => m.MessageType.EndsWith("PortCall", StringComparison.Ordinal))).PayloadJson);
        }
        finally { ext.Unsubscribe(token); }
    }

    [Fact]
    public async Task A_Dead_Letter_Is_Structured_Too()
    {
        using var system = ActorSystem.Create("typed-dead", ConfigurationFactory.ParseString(Hocon(false)));
        var ext = BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system);
        var reader = ext.Subscribe(null, typedPayload: true, out var token);
        try
        {
            var doomed = Tapped(system, "doomed");
            await doomed.GracefulStop(TimeSpan.FromSeconds(3));
            doomed.Tell(new PortCall(9, "Late"));
            var tap = await Next(reader, m => m.IsDeadLetter && m.MessageType.EndsWith("PortCall", StringComparison.Ordinal));
            Assert.Equal(9, tap.PayloadJson!.Value.GetProperty("ShipId").GetInt32());
        }
        finally { ext.Unsubscribe(token); }
    }

    [Theory]
    [InlineData("""{ "typedPayload": true }""", true)]
    [InlineData("""{ "typedPayload": "yes" }""", false)]
    [InlineData("{}", false)]
    [InlineData("nope", false)]
    public void The_Request_Asks_For_Structured_Payloads_Only_With_True(string body, bool expected)
    {
        Assert.Equal(expected, BowireAkkaProtocol.ReadTypedPayload([body]));
    }

    [Fact]
    public void Serialized_To_The_Stream_The_Structured_Payload_Is_Json_Not_A_String()
    {
        using var doc = JsonDocument.Parse("""{ "a": 1 }""");
        var tap = new TappedMessage("r", "s", "T", "p", DateTime.UtcNow, PayloadJson: doc.RootElement.Clone());
        using var wire = JsonDocument.Parse(JsonSerializer.Serialize(tap));
        Assert.Equal(1, wire.RootElement.GetProperty("PayloadJson").GetProperty("a").GetInt32());
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

/// <summary>A property that cannot be read, so neither serializer gets through it.</summary>
internal sealed class Unserializable(int value = 0)
{
    public int Boom => value == 0 ? throw new InvalidOperationException("no") : value;
}

#pragma warning disable CA1812 // instantiated by Akka via reflection
/// <summary>A serializer whose output is not JSON, as a protobuf binding would be.</summary>
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
