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
- **Bowire streaming pane** — `BowireAkkaProtocol` exposes two server-streaming methods: `Tap/MonitorMessages` yields `TappedMessage` envelopes as JSON, `Tap/Throughput` messages per second per actor.

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

**Per actor** — surgical, only the actors you name are tapped:

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

Both modes capture dead letters, and they can be combined — an actor that asks for the named tap mailbox explicitly is tapped once, not twice. The dead-letter bridge is created when the first Bowire client subscribes, so it never runs into the actor system's own bootstrap (up to 1.0.21 it was spawned at bootstrap, which the global mode made fail — dead letters were silently missing there). Should creating it ever fail, the actor system's log says so and the next subscriber tries again.

### 3. Watch in Bowire

Open the Bowire workbench (`/bowire` in embedded mode, or the `bowire` CLI), pick the **Akka.NET** tab, and stream `Tap/MonitorMessages`. Every message landing in a tapped mailbox — and every dead letter — appears in real time.

### Throughput per actor

`Tap/Throughput` sends a snapshot every interval: the actors that received messages in it, busiest first, each with its count, messages per second and a running total since the stream was opened. The request body is optional:

```json
{ "intervalMs": 1000, "top": 50 }
```

`intervalMs` is clamped to 100–60000; `top: 0` lists every actor. A snapshot looks like:

```json
{
  "Timestamp": "2026-09-28T20:30:01.004Z",
  "IntervalSeconds": 1.002,
  "Actors": [
    { "Path": "akka://Harbor/user/harbor-master", "Messages": 412, "PerSecond": 411.2, "Total": 3950 },
    { "Path": "akka://Harbor/user/dock-1", "Messages": 37, "PerSecond": 36.9, "Total": 402 }
  ]
}
```

The counts are taken where the tap sees the message, not from the message stream: that stream drops the oldest entries when its reader falls behind, and a figure read off it would be lowest exactly when an actor is busiest. Dead letters count under the dead-letter path. As with the message stream, nothing is counted while nobody watches.

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
