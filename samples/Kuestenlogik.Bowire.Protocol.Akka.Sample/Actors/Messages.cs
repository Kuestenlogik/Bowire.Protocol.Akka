// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;

namespace Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors;

public sealed record ScheduleArrival(int ShipId, string ShipName);
public sealed record DockShip(int ShipId, string ShipName);
public sealed record StartUnload(int ShipId);
public sealed record UnloadComplete(int ShipId);
public sealed record PortCallClosed(int ShipId, TimeSpan TotalDuration);
public sealed record BindDock(IActorRef Dock);
