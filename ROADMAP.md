# Roadmap

Version numbers are the published NuGet package versions of
`Kuestenlogik.Bowire.Protocol.Akka`, which track the git release tags.

## Shipped

- **1.0.0** — embedded mode, `EventStream`-style mailbox tap, JSON envelope
  of recipient / sender / message type / payload / timestamp.
- **1.0.1** — `DeadLetters` capture via `EventStream` subscription, with the
  `IsDeadLetter` flag on the envelope.
- **1.0.2 – 1.0.21** — dependency, packaging, and CI maintenance; no
  protocol changes.
- **1.1.0** *(current)* — everything planned for 1.1.0 and 1.2.0, in one
  release:
  - `Tap/Throughput` — messages per second per actor, counted at the tap (#29).
  - `Tap/Mailboxes` — depth and head of each tap mailbox, nothing dequeued (#28).
  - A filter on `Tap/MonitorMessages` per actor path and message type,
    applied before the stream's buffer (#31).
  - `PayloadJson` in a format the user picks — auto, properties, fields
    (reflection), or what the configured serializer puts on the wire (#30).
  - `Tap/Tell` — send into the actor system, only what the host allows (#32).
  - New package `Kuestenlogik.Bowire.Protocol.Akka.Remote` — the standalone
    `bowire` CLI attaches to a running cluster over ClusterClient (#27).
  - Dead letters also in the global-default mailbox mode, the documented wire
    keys match the code, tests for every wiring mode (#33, #34, #36).

## Planned

Nothing scheduled. Candidates: tapping cluster membership events, and the
mailbox view and throughput over the remote tap (today it relays the message
stream only).

---

Planned work is tracked as issues on the Kuestenlogik
**[Bowire project board](https://github.com/orgs/Kuestenlogik/projects)**,
not in this repository's issue tracker.
