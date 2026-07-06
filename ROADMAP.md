# Roadmap

Version numbers are the published NuGet package versions of
`Kuestenlogik.Bowire.Protocol.Akka`, which track the git release tags.

## Shipped

- **1.0.0** — embedded mode, `EventStream`-style mailbox tap, JSON envelope
  of recipient / sender / message type / payload / timestamp.
- **1.0.1** — `DeadLetters` capture via `EventStream` subscription, with the
  `IsDeadLetter` flag on the envelope.
- **1.0.2 – 1.0.11** *(current)* — dependency, packaging, and CI maintenance;
  no protocol changes.

## Planned

- **1.1.0** — external `Akka.Cluster.Tools.ClusterClient` transport so the
  standalone `bowire` CLI can attach to a running cluster, plus
  mailbox-snapshot inspection (size, head messages) and per-actor throughput
  stats.
- **1.2.0** — typed payload via Akka serializer roundtrip, an opt-in filter
  API from the Bowire UI (per actor path, per message type), and
  Tell-from-Bowire (interactive duplex).

---

Planned work is tracked as issues on the Kuestenlogik
**[Bowire project board](https://github.com/orgs/Kuestenlogik/projects)**,
not in this repository's issue tracker.
