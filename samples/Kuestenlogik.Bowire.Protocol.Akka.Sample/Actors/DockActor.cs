// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using global::Akka.Actor;
using global::Akka.Event;

namespace Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors;

#pragma warning disable CA1812 // Akka instantiates actors via reflection
internal sealed class DockActor : UntypedActor
{
    public static Props Build(IActorRef crane, IActorRef master)
        => Props.Create(() => new DockActor(crane, master));

    private readonly IActorRef _crane;
    private readonly IActorRef _master;
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly Dictionary<int, DateTime> _arrivals = [];

    public DockActor(IActorRef crane, IActorRef master)
    {
        _crane = crane;
        _master = master;
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case DockShip ds:
                _arrivals[ds.ShipId] = DateTime.UtcNow;
                _log.Info("Dock receiving {0}", ds.ShipName);
                _crane.Tell(new StartUnload(ds.ShipId), Self);
                break;
            case UnloadComplete uc when _arrivals.Remove(uc.ShipId, out var arrived):
                _master.Tell(new PortCallClosed(uc.ShipId, DateTime.UtcNow - arrived));
                break;
        }
    }
}
#pragma warning restore CA1812
