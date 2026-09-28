# Roadmap

Version numbers are the published NuGet package versions of
`Kuestenlogik.Bowire.Protocol.Akka`, which track the git release tags.

## Shipped

- **1.0.0** — embedded mode, `EventStream`-style mailbox tap, JSON envelope
  of recipient / sender / message type / payload / timestamp.
- **1.0.1** — `DeadLetters` capture via `EventStream` subscription, with the
  `IsDeadLetter` flag on the envelope.
- **1.0.2 – 1.0.21** *(latest release)* — dependency, packaging, and CI
  maintenance; no protocol changes.

## Done, not yet released

- Dead letters also in the global-default mailbox mode, the documented wire
  keys match the code, tests for every wiring mode (#33, #34, #36).
- `Tap/Throughput` — messages per second per actor, counted at the tap (#29).
- `Tap/Mailboxes` — depth and head of each tap mailbox, nothing dequeued (#28).
- A filter on `Tap/MonitorMessages` per actor path and message type, applied
  before the stream's buffer (#31).
- `PayloadJson` in a format the user picks — auto, properties, fields
  (reflection), or what the configured serializer puts on the wire (#30).
- `Tap/Tell` — send into the actor system, only what the host allows (#32).
- `Kuestenlogik.Bowire.Protocol.Akka.Remote` — the standalone `bowire` CLI
  attaches to a running cluster over ClusterClient (#27).

What was planned for 1.1.0 and 1.2.0 is all in this list.

---

Planned work is tracked as issues on the Kuestenlogik
**[Bowire project board](https://github.com/orgs/Kuestenlogik/projects)**,
not in this repository's issue tracker.
