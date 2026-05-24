# Kuestenlogik.Bowire.Protocol.Akka

[![CI](https://img.shields.io/github/actions/workflow/status/Kuestenlogik/Bowire.Protocol.Akka/ci.yml?branch=main&label=CI)](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Akka/branch/main/graph/badge.svg)](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Akka)
[![NuGet](https://img.shields.io/nuget/v/Kuestenlogik.Bowire.Protocol.Akka)](https://www.nuget.org/packages/Kuestenlogik.Bowire.Protocol.Akka)
[![License](https://img.shields.io/github/license/Kuestenlogik/Bowire.Protocol.Akka)](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/LICENSE)
[![Bowire](https://img.shields.io/badge/Bowire-%E2%89%A5%201.5.0%2C%20%3C%202.0-006B9F)](https://github.com/Kuestenlogik/Bowire/blob/main/docs/architecture/compatibility.md)

Bowire protocol plugin for **[Akka.NET](https://getakka.net/)** actor systems. Streams every message that lands in a tap-mailboxed actor's mailbox into the [Bowire](https://github.com/Kuestenlogik/Bowire) workbench, so you can watch a live actor system the same way you watch gRPC streams or MQTT topics.

## What it does

- **Mailbox tap** — a custom Akka.NET `MailboxType` (`BowireTapMailbox`) wraps the standard unbounded queue and forwards every enqueue to a per-actor-system extension. Opt-in globally (default mailbox swap) or per-actor (`Props.WithMailbox(...)`).
- **`IExtension` integration** — `BowireAkkaExtension` owns the broadcast channel and the active subscriber list. Steady-state cost when nobody's watching: one volatile read per message.
- **DeadLetters capture** — the extension subscribes to the actor system's `EventStream` and republishes every `Akka.Event.DeadLetter` through the same channel with `TappedMessage.IsDeadLetter = true`, so undeliverable messages surface in the Bowire stream without any per-actor opt-in.
- **Bowire streaming pane** — `BowireAkkaProtocol` exposes one server-streaming method, `Tap/MonitorMessages`, that yields `TappedMessage` envelopes (recipient path, sender path, CLR type, payload, timestamp, dead-letter flag) as JSON.

## Install

```bash
dotnet add package Kuestenlogik.Bowire.Protocol.Akka
```

## Use

### 1. Register the actor system in DI

```csharp
using Akka.Actor;
using Microsoft.Extensions.DependencyInjection;

var system = ActorSystem.Create("MyApp", hocon);
builder.Services.AddSingleton(system);
builder.Services.AddBowire(); // picks up Kuestenlogik.Bowire.Protocol.Akka via plugin discovery
```

### 2. Opt actors into the tap mailbox

**Globally** — every actor created after this gets tapped:

```hocon
akka.actor.default-mailbox.mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
```

**Per-actor** — surgical, leaves other actors at their default mailbox:

```hocon
akka.actor.bowire-tap = {
  mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
}
```

```csharp
var orders = system.ActorOf(
    Props.Create<OrdersActor>().WithMailbox("akka.actor.bowire-tap"),
    "orders");
```

### 3. Watch in Bowire

Open the Bowire workbench (`/bowire` in embedded mode or `bowire` CLI), pick the **Akka.NET** tab, and start streaming `Tap/MonitorMessages`. Every message landing in a tapped mailbox lands in your stream pane in real time.

## Sample

A runnable end-to-end sample lives under [`samples/Kuestenlogik.Bowire.Protocol.Akka.Sample`](samples/Kuestenlogik.Bowire.Protocol.Akka.Sample) — three actors arranged in a small harbour workflow plus a 2-second port-call ticker, so the live message stream is never quiet.

```bash
dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Akka.Sample
```

## Roadmap

- **0.1.0** — embedded mode, `EventStream`-style mailbox tap, JSON envelope of recipient/sender/type/payload/timestamp.
- **0.2.0** (current) — `DeadLetters` capture via `EventStream` subscription with `IsDeadLetter` flag on the envelope.
- **0.3.0** — external `Akka.Cluster.Tools.ClusterClient` transport so the standalone `bowire` CLI can attach to a running cluster, mailbox-snapshot inspection (size, head messages), per-actor throughput stats.
- **0.4.0** — typed payload via Akka serializer roundtrip, opt-in filter API from the Bowire UI (per actor path, per message type), Tell-from-Bowire (interactive duplex).

## License

[Apache-2.0](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/LICENSE)
