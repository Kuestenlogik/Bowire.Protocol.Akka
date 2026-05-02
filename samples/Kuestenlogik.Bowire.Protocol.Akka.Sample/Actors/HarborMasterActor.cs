// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using global::Akka.Actor;
using global::Akka.Event;

namespace Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors;

#pragma warning disable CA1812 // Akka instantiates actors via reflection
internal sealed class HarborMasterActor : UntypedActor
{
    public static Props Build() => Props.Create(() => new HarborMasterActor());

    private IActorRef _dock = ActorRefs.Nobody;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case BindDock bd:
                _dock = bd.Dock;
                break;
            case ScheduleArrival sa:
                _log.Info("Master scheduling {0}", sa.ShipName);
                _dock.Tell(new DockShip(sa.ShipId, sa.ShipName));
                break;
            case PortCallClosed pc:
                _log.Info("Port call for ship {0} done in {1:0.0}s", pc.ShipId, pc.TotalDuration.TotalSeconds);
                break;
        }
    }
}
#pragma warning restore CA1812
