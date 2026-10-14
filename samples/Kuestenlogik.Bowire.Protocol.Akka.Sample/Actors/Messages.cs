// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;

namespace Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors;

internal sealed record ScheduleArrival(int ShipId, string ShipName);
internal sealed record DockShip(int ShipId, string ShipName);
internal sealed record StartUnload(int ShipId);
internal sealed record UnloadComplete(int ShipId);
internal sealed record PortCallClosed(int ShipId, TimeSpan TotalDuration);
internal sealed record BindDock(IActorRef Dock);
