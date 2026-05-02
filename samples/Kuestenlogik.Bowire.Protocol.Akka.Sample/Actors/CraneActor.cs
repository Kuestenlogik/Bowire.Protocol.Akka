// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using global::Akka.Actor;
using global::Akka.Event;

namespace Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors;

#pragma warning disable CA1812 // Akka instantiates actors via reflection
internal sealed class CraneActor : UntypedActor, IWithTimers
{
    public static Props Build() => Props.Create(() => new CraneActor());

    public ITimerScheduler Timers { get; set; } = null!;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case StartUnload su:
                _log.Info("Crane lifting cargo from {0}", su.ShipId);
                // Schedule UnloadComplete back to the dock after a fake
                // 800 ms lift. Sending to Sender keeps the call chain
                // visible in the tap stream.
                var dock = Sender;
                Timers.StartSingleTimer(
                    key: $"unload-{su.ShipId}",
                    msg: new TimedCompletion(su.ShipId, dock),
                    timeout: TimeSpan.FromMilliseconds(800));
                break;
            case TimedCompletion tc:
                tc.Dock.Tell(new UnloadComplete(tc.ShipId), Self);
                break;
        }
    }

    private sealed record TimedCompletion(int ShipId, IActorRef Dock);
}
#pragma warning restore CA1812
