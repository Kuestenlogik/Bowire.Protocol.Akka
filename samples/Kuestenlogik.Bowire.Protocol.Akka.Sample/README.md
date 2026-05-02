# Sample — Akka.NET tap

A small ASP.NET Core host that boots an Akka.NET `ActorSystem` of three
actors and pipes every mailbox enqueue into the Bowire workbench's
**Akka.NET** tab.

```
HarborMasterActor ──ScheduleArrival──▶ DockActor ──StartUnload──▶ CraneActor
        ▲                                                                │
        └──────────── PortCallClosed ◀── UnloadComplete ◀────────────────┘
```

A background ticker schedules a fresh port call every two seconds, so
the live message stream never goes quiet.

## Run

```sh
dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Akka.Sample
```

Open <http://localhost:5080/bowire>, switch to the **Akka.NET** tab,
and stream `Tap → MonitorMessages`.

## How the tap is wired

The plug-in ships a custom `MailboxType` that wraps Akka's
`UnboundedMessageQueue`. Every `Enqueue` is forwarded to a process-wide
`BowireAkkaExtension` which fans out to subscribed Bowire clients via
bounded channels with drop-oldest back-pressure, so a slow viewer never
stalls the actor system.

The sample wires the tap **globally** via HOCON in `Program.cs`:

```hocon
akka.actor.default-mailbox.mailbox-type =
    "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
```

Every actor in this `ActorSystem` inherits the tap mailbox automatically
— no per-actor `Props.WithMailbox(...)` needed. For per-actor opt-in,
see the plug-in README.
