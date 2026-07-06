# Akka.NET coverage

What this plugin covers from the Akka.NET surface, and what it deliberately doesn't (yet).

## Tap surface — what shows up in the Bowire stream

| Source | Carrier | Tested | Notes |
|--------|---------|:-:|-------|
| **Mailbox enqueue** | `BowireTapMailbox` → `BowireTapMessageQueue` | ✅ | Custom `MailboxType` wraps the standard unbounded queue. Every `Enqueue` forwards a `TappedMessage` to the per-`ActorSystem` extension. |
| **DeadLetters** | `BowireAkkaExtension/DeadLetterListener` | ✅ | Subscribes to the system's `EventStream`, republishes every `Akka.Event.DeadLetter` as a `TappedMessage` with `IsDeadLetter = true`. |
| Cluster gossip / membership | `Akka.Cluster.Tools.ClusterClient` | ⏳ | Parked on the 1.1.0 plugin roadmap. |
| Persistence write / recovery events | journal hooks | ⏳ | Same — 1.1.0+. |
| ActorSystem lifecycle (start, terminate) | `EventStream` `Terminated` | ⏳ | Optional addition once the cluster track lands. |

## Opt-in modes

| Mode | Wiring | Tested |
|------|--------|:-:|
| **Per-actor mailbox** | `Props.WithMailbox("akka.actor.bowire-tap")` + HOCON config block | ✅ |
| **Global default mailbox** | `akka.actor.default-mailbox.mailbox-type = "...BowireTapMailbox..."` | ✅ |
| **Mixed** (default tap + actor-specific override) | Both wirings active simultaneously | ⏳ Single-mode tests only — intentional, the modes are independent. |

## Tap envelope (`TappedMessage`)

| Field | Source | Tested |
|-------|--------|:-:|
| `RecipientPath` | `IActorRef.Path` of the receiving actor | ✅ |
| `SenderPath` | `IActorRef.Path` of the sender (or `Sender == null` → empty) | ✅ |
| `MessageType` | CLR type name (FQN) of the message | ✅ |
| `Payload` | JSON-serialised view of the message | ✅ |
| `Timestamp` | UTC `DateTimeOffset` at enqueue (or `EventStream` raise for DeadLetters) | ✅ |
| `IsDeadLetter` | `true` for `EventStream`-republished `DeadLetter`s, else `false` | ✅ |

## Plugin contract — `IBowireProtocol`

| Method | Behaviour | Tested |
|--------|-----------|:-:|
| `DiscoverAsync` | Returns one synthetic service `Tap` with one method `MonitorMessages` (server-streaming). No live ActorSystem needed at discover time. | ✅ |
| `InvokeStreamAsync` | Hooks the `BowireAkkaExtension`'s broadcast channel and yields each `TappedMessage` as JSON until cancellation. Multi-subscriber safe. | ✅ |
| `InvokeAsync` | Returns synthetic "use the streaming method" guidance — Akka.NET doesn't have a unary-RPC concept that maps to the workbench's invoke pane. | ✅ |
| `OpenChannelAsync` | Returns `null` — no duplex-channel semantics for actor messages. | ✅ |

## Coverage measurement

Run from the repo root:

```bash
dotnet test --collect:"XPlat Code Coverage" --results-directory artifacts/cov
```

Latest snapshot (xunit.v3 + Akka.TestKit, 8 tests):

| Component | Line | Branch |
|-----------|:----:|:------:|
| `BowireTapMailbox` | 100 % | 100 % |
| `BowireAkkaExtensionProvider` | 100 % | 100 % |
| `BowireAkkaExtension/DeadLetterListener` | 100 % | 100 % |
| `BowireTapMessageQueue` | 87 % | 62 % |
| `BowireAkkaExtension` | 90 % | 59 % |
| `BowireAkkaProtocol` | 77 % | 75 % |
| **Package total** | **87 %** | **62 %** |

The branch gap is the optional-subscriber paths — branches that fire only when 0 / 1 / many subscribers are attached at the moment of an enqueue, which the steady-state test suite doesn't multiplex against. Lifting them needs an integration-style test that spins up two `InvokeStreamAsync` consumers in parallel; on the 1.1.0 list.
