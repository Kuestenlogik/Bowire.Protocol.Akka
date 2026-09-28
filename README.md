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
- **Bowire streaming pane** — `BowireAkkaProtocol` exposes two server-streaming methods — `Tap/MonitorMessages` yields `TappedMessage` envelopes as JSON, `Tap/Throughput` messages per second per actor — and one unary method, `Tap/Mailboxes`, that shows what is waiting in each tapped mailbox.

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

To see only part of it, give the request a filter; every field is optional:

```json
{ "paths": ["/user/dock-*"], "messageTypes": ["PortCall*"], "includeDeadLetters": false }
```

`*` stands for any run of characters and a pattern matches the whole value. A path pattern matches the full path (`akka://Harbor/user/dock-*`) or the path from `/user` on; a type pattern the full type name or the simple one. Several patterns of one kind are alternatives, and paths and types must both match. A dead letter's recipient is the dead-letter actor, so a path filter lets dead letters through only if it names that path.

The filter runs before your stream's buffer, not after it: under load the buffer drops its oldest entries, and what the filter keeps out never takes a place there. A narrow filter therefore also keeps the messages you asked for from being dropped.

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

### What is waiting in a mailbox

`Tap/Mailboxes` (in the invoke pane) lists every tapped mailbox with its depth and the oldest queued messages, deepest first:

```json
{ "path": "akka://Harbor/user/dock", "head": 5, "nonEmptyOnly": true }
```

All fields are optional; `head` is clamped to 0–100 (default 5). Nothing is taken off a queue: the head is read from a snapshot, and the actor goes on processing every message in the order it was sent. The view knows a mailbox from the moment the actor is created until it stops — registering it is the only cost, once per actor, none per message.

### Sending a message (off by default)

`Tap/Tell` sends a message to an actor and shows what it sends back — as a duplex channel, or once from the invoke pane (with `replyTimeoutMs` it waits for the first reply). It exists only when the host allows it; the workbench cannot switch it on. Allow it in code:

```csharp
BowireAkkaExtensionProvider.Instance.Apply((ExtendedActorSystem)system)
    .EnableTell(new TellPolicy(
        paths: ["/user/dock-*"],
        messageTypes: [typeof(ScheduleArrival), typeof(CloseBerth)]));
```

or in HOCON:

```hocon
bowire.akka.tell {
  paths = ["/user/dock-*"]
  message-types = ["Harbor.Messages.ScheduleArrival, Harbor"]
}
```

A request names a concrete actor, one of the listed types, and the message as JSON:

```json
{ "path": "/user/dock-1", "messageType": "Harbor.Messages.ScheduleArrival", "message": { "shipId": 101 } }
```

What the policy decides is all that is decided:

- **Only listed types.** The JSON is deserialized into exactly the type chosen from the list; a `$type` in the body is ignored, so a request cannot name a type the host did not list.
- **Only concrete, local paths.** An actor selection understands more than a path — `*` sends to every match, `..` climbs to the parent (`/user/dock-1/../admin` would pass a `/user/dock-*` pattern), another address sends over the network. None of that is accepted.
- **Every tell is logged** in the actor system's own log, with path and type; enabling Tell logs a warning naming what is allowed.
- `DisableTell()` takes the permission back; an open channel refuses from then on.

The tells come from an actor created for the channel, so a reply to `Sender` arrives in the channel rather than in dead letters.

## Standalone: attach the bowire CLI to a running cluster

Everything above runs embedded — the workbench inside the host. To watch a cluster from the standalone `bowire` CLI instead, add the separate package **`Kuestenlogik.Bowire.Protocol.Akka.Remote`** (it brings Akka.Cluster.Tools; embedded-only users don't need it).

In the host, which has to run Akka.Cluster, opt in:

```csharp
using Kuestenlogik.Bowire.Protocol.Akka.Remote;

system.EnableBowireRemoteTap();
```

That starts a relay at `/user/bowire-tap-relay` and registers it with the ClusterClient receptionist. The CLI, with the same package installed as a plugin, connects to a contact point of the cluster:

```bash
bowire --url "akka.tcp://Harbor@10.0.0.5:4053?clientHost=10.0.0.9"
```

and offers `Tap/MonitorMessages` with the same filter and `payloadFormat` as embedded — the filter and the rendering run in the host, so only what was asked for crosses the network. The CLI runs a small actor system of its own that the relay sends back to; `clientHost` is the name it binds and advertises, and it has to be reachable from the cluster (default `localhost`).

- **Read-only.** The remote tap relays taps; it never sends into the cluster. `Tap/Tell` is embedded-only.
- **Off unless enabled**, and enabling logs a warning.
- **Message contents cross the network.** Akka remoting is not authenticated unless you configure TLS — enable the relay where the network is trusted, or with remoting secured.
- **A CLI that goes away stops being served**: it renews its subscription every 10 s and the relay drops one not renewed for 30 s (`bowire.akka.remote-tap { lease = 30s, heartbeat = 10s }` in the host). The relay's heartbeat keeps ClusterClient's response tunnel, which closes after 30 s of silence, open in quiet times.

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

`Payload` is the message's `ToString()` — always there, whatever the message is. For its fields, pick a `payloadFormat` in the `MonitorMessages` request (the workbench offers it as a drop-down; it combines with the filter). Each message then also carries `PayloadJson`, the message as a JSON object:

| `payloadFormat` | `PayloadJson` |
|---|---|
| `none` (default) | not sent |
| `auto` | the most readable rendering that works: a JSON serializer the actor system binds to the type on purpose; protobuf's own JSON mapping for a protobuf message (found by reflection — the plugin has no protobuf dependency); the public properties; the instance fields when the properties show nothing; Akka's default JSON as the last resort |
| `properties` | the public properties |
| `fields` | the instance fields, public and private, read by reflection — the object's actual state, also for a class that exposes none of it (auto-property backing fields under the property's name; cycles and depth are cut off) |
| `akka` | what the configured serializer would put on the wire: its JSON as is — for Akka's default a type-preserving format where `17` is `{"$": "I17"}` — or, for a binary serializer such as protobuf or Hyperion, `{ "serializer": ..., "bytes": ..., "base64": ... }` |

The tap sees the message before anything serializes it — inside one process Akka never does — so protobuf or Hyperion bindings need no decoding; they only matter for `akka`, which shows what they would send.

Rendering happens inside the sender's `Tell`, so it only happens while somebody asked for it, and once per message and format however many readers did. A message that will not render in the chosen format, or whose JSON is above 256 KB, has `PayloadJson: null` and is delivered as usual.

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
