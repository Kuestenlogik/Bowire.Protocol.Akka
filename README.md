# Kuestenlogik.Bowire.Protocol.Akka

[![CI](https://img.shields.io/github/actions/workflow/status/Kuestenlogik/Bowire.Protocol.Akka/ci.yml?branch=main&label=CI)](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Akka/branch/main/graph/badge.svg)](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Akka)
[![NuGet](https://img.shields.io/nuget/v/Kuestenlogik.Bowire.Protocol.Akka)](https://www.nuget.org/packages/Kuestenlogik.Bowire.Protocol.Akka)
[![License](https://img.shields.io/github/license/Kuestenlogik/Bowire.Protocol.Akka)](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/LICENSE)
[![Bowire](https://img.shields.io/badge/Bowire-%E2%89%A5%202.2.1%2C%20%3C%203.0-006B9F)](https://github.com/Kuestenlogik/Bowire/blob/main/docs/architecture/compatibility.md)

Bowire protocol plugin for **[Akka.NET](https://getakka.net/)** actor systems. Streams every message that lands in a tap-mailboxed actor's mailbox — plus the actor system's dead letters — into the [Bowire](https://github.com/Kuestenlogik/Bowire) workbench, so you can watch a live actor system the same way you watch gRPC streams or MQTT topics.

## What it does

- **Mailbox tap** — a custom Akka.NET `MailboxType` (`BowireTapMailbox`) wraps the standard unbounded queue and forwards every enqueue to a per-actor-system extension. Opt in globally (default-mailbox swap) or per actor (`Props.WithMailbox(...)`).
- **DeadLetters capture** — the extension subscribes to the actor system's `EventStream` and republishes every `Akka.Event.DeadLetter` through the same channel with `IsDeadLetter = true`, so undeliverable messages surface without any per-actor opt-in.
- **`IExtension` integration** — `BowireAkkaExtension` owns the active subscriber list and the dead-letter bridge. When nobody is watching, each enqueue costs a single subscriber-count check; the message is only marshalled once at least one subscriber is attached.
- **Bowire streaming pane** — `BowireAkkaProtocol` exposes one server-streaming method, `Tap/MonitorMessages`, that yields `TappedMessage` envelopes as JSON.

## How it works

```
actor mailbox ─enqueue─▶ BowireTapMailbox ─▶ BowireAkkaExtension ─fan-out─▶ subscriber channels ─▶ Tap/MonitorMessages ─JSON─▶ Bowire UI
                                                    ▲
             EventStream DeadLetter ────────────────┘
```

`BowireTapMailbox` wraps Akka's `UnboundedMessageQueue`; the dequeue path is untouched, so the tap never changes delivery order or semantics. On each enqueue it hands a `TappedMessage` to the process-wide `BowireAkkaExtension`, which fans out to every subscribed Bowire client over a bounded, drop-oldest channel — a slow viewer can never stall the actor system. Dead letters reach the same fan-out through an `EventStream` subscription.

## Requirements

- .NET 10
- Akka.NET ≥ 1.5
- A Bowire-enabled host — `Kuestenlogik.Bowire` ≥ 2.2.1, < 3.0 (see the [compatibility matrix](https://github.com/Kuestenlogik/Bowire/blob/main/docs/architecture/compatibility.md))

## Install

```sh
dotnet add package Kuestenlogik.Bowire.Protocol.Akka
```

## Use

### 1. Register the actor system in DI

```csharp
using Akka.Actor;
using Microsoft.Extensions.DependencyInjection;

var system = ActorSystem.Create("MyApp", hocon);
builder.Services.AddSingleton(system);
builder.Services.AddBowire(); // discovers this plugin automatically
```

### 2. Opt actors into the tap mailbox

**Per actor** — surgical, and keeps dead-letter capture working:

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

**Globally** — every actor created afterwards is tapped:

```hocon
akka.actor.default-mailbox.mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
```

> **Note** — as the *global* default mailbox, `BowireTapMailbox` is created for the root guardian during bootstrap, before the actor system is navigable. The extension degrades gracefully there: live mailbox taps work end-to-end, but **dead-letter capture is silently disabled**. Use the per-actor (or a named-mailbox) opt-in if you need dead letters in the stream.

### 3. Watch in Bowire

Open the Bowire workbench (`/bowire` in embedded mode, or the `bowire` CLI), pick the **Akka.NET** tab, and stream `Tap/MonitorMessages`. Every message landing in a tapped mailbox — and every dead letter — appears in real time.

## The envelope

Each observation is a `TappedMessage`, serialized to JSON:

```json
{
  "Recipient": "akka://Harbor/user/dock-1",
  "Sender": "akka://Harbor/user/harbor-master",
  "MessageType": "Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors.ScheduleArrival",
  "Payload": "ScheduleArrival { ShipId = 101, ShipName = Nordstern }",
  "Timestamp": "2026-07-06T09:14:22.187Z",
  "IsDeadLetter": false
}
```

`Payload` is a best-effort `ToString()` rendering today; typed serializer round-tripping is on the [roadmap](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/ROADMAP.md).

## Sample

A runnable end-to-end sample lives under [`samples/Kuestenlogik.Bowire.Protocol.Akka.Sample`](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/tree/main/samples/Kuestenlogik.Bowire.Protocol.Akka.Sample) — three actors in a small harbour workflow plus a 2-second port-call ticker, so the live stream is never quiet.

```sh
dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Akka.Sample
```

Then open <http://localhost:5080/bowire> and stream the Akka.NET tab.

## Documentation

- [ROADMAP.md](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/ROADMAP.md) — shipped and planned versions
- [COVERAGE.md](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/COVERAGE.md) — what the plugin taps from Akka's surface, and what it deliberately doesn't (yet)

## License

[Apache-2.0](https://github.com/Kuestenlogik/Bowire.Protocol.Akka/blob/main/LICENSE)
