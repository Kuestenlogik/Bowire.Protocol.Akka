// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Akka.Actor;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// One observation of a message landing in (or sent to) an actor's mailbox.
/// Captured by <see cref="BowireTapMessageQueue"/> on enqueue — or by the
/// <see cref="BowireAkkaExtension"/>'s <c>DeadLetter</c> subscription on
/// undelivered envelopes — and forwarded to the broadcast channel.
/// </summary>
/// <param name="Recipient">Absolute actor path of the receiver.</param>
/// <param name="Sender">Absolute path of the sender, or <c>deadLetters</c>.</param>
/// <param name="MessageType">CLR type name of the payload (no assembly).</param>
/// <param name="Payload">
/// Best-effort string rendering of the message — <see cref="object.ToString"/>
/// for now; future iterations can pluggably serialize via Akka's serializer
/// or System.Text.Json for richer inspection.
/// </param>
/// <param name="Timestamp">UTC timestamp of the enqueue.</param>
/// <param name="IsDeadLetter">
/// True when this observation came from the actor system's
/// <c>Akka.Event.EventStream</c> as an <c>Akka.Event.DeadLetter</c> rather
/// than from a tapped mailbox enqueue. Lets the Bowire UI style
/// undeliverable messages distinctly. Defaults to <c>false</c> for
/// backwards compatibility.
/// </param>
public sealed record TappedMessage(
    string Recipient,
    string Sender,
    string MessageType,
    string Payload,
    DateTime Timestamp,
    bool IsDeadLetter = false);
