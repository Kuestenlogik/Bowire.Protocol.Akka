# End-to-end smoke test

Verifies the whole chain: **pack Bowire → install plugin → run sample → see TappedMessages stream in the workbench**. Takes about 60 seconds the first time (most of it NuGet restore), <10 s on repeat runs.

## Prerequisites

- Windows / Linux / macOS with the .NET SDK matching [`global.json`](global.json).
- Bowire main repo checked out as a sibling directory (`../Bowire`).
- `bowire` CLI installed as a global tool — from the main repo:
  ```bash
  dotnet tool install --global --add-source ./artifacts/packages bowire
  ```
  Or build-and-run without installing:
  `dotnet run --project ../Bowire/src/Kuestenlogik.Bowire.Tool` plus the subcommand.

## Steps

### 1. Pack Bowire core + this plugin

```bash
# in ../Bowire
dotnet pack -c Release

# in this repo
dotnet pack -c Release
```

Both `dotnet pack` invocations land in their respective `artifacts/packages/`.

### 2. Install the plugin

```bash
bowire plugin install Kuestenlogik.Bowire.Protocol.Akka --source ../Bowire/artifacts/packages --source ./artifacts/packages
bowire plugin list
```

`plugin list` should show `Kuestenlogik.Bowire.Protocol.Akka` with the version that just got packed.

### 3. Run the sample

```bash
dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Akka.Sample
```

The sample boots an `ActorSystem("Harbor")` with `BowireTapMailbox` as the global default mailbox and starts a background ticker that schedules a fresh port-call workflow every two seconds. Each workflow pushes ~6 messages through three actors (HarborMaster → Dock → Crane → Harborm aster), so the live stream sees ~3 messages per second steady-state.

Sample listens on `http://localhost:5080/bowire`. The terminal should log `Now listening on: http://localhost:5080`.

### 4. Watch the stream in Bowire

Browse to `http://localhost:5080/bowire`, pick the **Akka.NET** protocol tab in the sidebar, click `Tap → MonitorMessages`, hit **Execute**.

You should see a continuous stream of frames like:

```json
{
  "RecipientPath": "akka://Harbor/user/harbor-master",
  "SenderPath": "akka://Harbor/user/dock-1",
  "MessageType": "Kuestenlogik.Bowire.Protocol.Akka.Sample.Actors.PortCallClosed",
  "Payload": { "PortCallId": 17, "DockId": 1 },
  "Timestamp": "2026-05-08T19:42:13.1234567+00:00",
  "IsDeadLetter": false
}
```

If you stop and immediately restart the streaming method, the new subscription picks up the next-emitted message — the broadcast channel discards messages emitted while no subscriber was attached, so the stream stays bounded.

### 5. Smoke test — DeadLetters

To verify the `DeadLetters` path: stop the sample, start it again with the global tap mailbox **disabled** (comment out the `TapHocon` and use `WithMailbox` per-actor instead). With the global default off, system-internal actors enqueue to the regular mailbox, so any message sent to a stopped or non-existent path lands on the `EventStream` as a `DeadLetter`. The plugin's `DeadLetterListener` republishes those with `"IsDeadLetter": true` — visible alongside the live mailbox messages in the same stream.

### 6. Tear down

`Ctrl+C` in the sample terminal.

```bash
bowire plugin uninstall Kuestenlogik.Bowire.Protocol.Akka
```

## What "passing" means

- The `bowire plugin install` step exits 0 and the package shows up in `plugin list`.
- The sample boots without `Akka` config errors (HOCON parse, missing extension, mailbox-not-found).
- The Bowire workbench shows the **Akka.NET** protocol tab.
- The streaming pane shows new frames every <1 second once **Execute** is clicked, with both `Tell` and `Forward` recipient paths surfacing.
- Stopping and restarting the streaming method picks up new messages — no leak of the previous subscription.

If any of those fails, the failure surface is one of: NuGet feed mis-configured (step 2), HOCON / extension wiring (step 3), workbench plugin discovery (step 4), or the broadcast-channel lifecycle (step 4 / restart). The unit tests in [`tests/`](tests/) fail-fast on the broadcast / lifecycle problems; the smoke test catches the integration glue around them.
