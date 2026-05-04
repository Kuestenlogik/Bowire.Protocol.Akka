// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;
using Akka.Configuration;
using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors;

// Three-actor harbour workflow that streams every message into the
// Bowire workbench's Akka.NET tab:
//
//   HarborMasterActor ──ScheduleArrival──▶ DockActor ──StartUnload──▶ CraneActor
//           ▲                                                                 │
//           └─────────── PortCallClosed ◀── UnloadComplete ◀──────────────────┘
//
// A background ticker schedules a fresh port call every two seconds so
// the live stream never goes quiet.
//
// Browse:
//   1. Open http://localhost:5080/bowire
//   2. Pick the "Akka.NET" protocol tab
//   3. Stream the Tap → MonitorMessages method

// Define a NAMED mailbox config (akka.actor.bowire-tap) and opt our
// three application actors into it via Props.WithMailbox below — this
// is the surgical opt-in pattern from the plugin README. Setting the
// default-mailbox globally would also rewrap the system's own root
// guardian + dead-letters mailbox during bootstrap, which loads the
// BowireAkkaExtension before the actor system is navigable.
const string TapHocon = """
    akka.actor.bowire-tap = {
        mailbox-type = "Kuestenlogik.Bowire.Protocol.Akka.BowireTapMailbox, Kuestenlogik.Bowire.Protocol.Akka"
    }
    """;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ActorSystem>(_ =>
    ActorSystem.Create("Harbor", ConfigurationFactory.ParseString(TapHocon)));
builder.Services.AddBowire();

var app = builder.Build();
app.MapBowire();

var system = app.Services.GetRequiredService<ActorSystem>();

// Master gets bound to the dock after construction (forward-decl), so
// PortCallClosed can flow back. Order: crane → master → dock → bind.
// Each actor opts into the BowireTapMailbox via WithMailbox so the
// system-internal actors keep their default mailbox.
var crane = system.ActorOf(CraneActor.Build().WithMailbox("akka.actor.bowire-tap"), "crane-A1");
var master = system.ActorOf(HarborMasterActor.Build().WithMailbox("akka.actor.bowire-tap"), "harbor-master");
var dock = system.ActorOf(DockActor.Build(crane, master).WithMailbox("akka.actor.bowire-tap"), "dock-1");
master.Tell(new BindDock(dock));

// Background scheduler — random ship every 2 s.
_ = Task.Run(async () =>
{
    var ships = new[]
    {
        (Id: 101, Name: "Nordstern"),
        (Id: 102, Name: "Isabella"),
        (Id: 103, Name: "Aurora"),
    };
    var rng = new Random();
    while (!app.Lifetime.ApplicationStopping.IsCancellationRequested)
    {
        var ship = ships[rng.Next(ships.Length)];
        master.Tell(new ScheduleArrival(ship.Id, ship.Name));
        await Task.Delay(2000, app.Lifetime.ApplicationStopping);
    }
});

app.Lifetime.ApplicationStopping.Register(() => system.Terminate().Wait(TimeSpan.FromSeconds(5)));

app.Run();
