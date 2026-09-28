// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>One tap mailbox at the moment it was looked at (#28).</summary>
/// <param name="Path">Absolute path of the actor that owns the mailbox.</param>
/// <param name="Depth">Messages queued, not yet processed.</param>
/// <param name="Head">The oldest queued messages, next to be processed first.</param>
public sealed record MailboxSnapshot(string Path, int Depth, IReadOnlyList<QueuedMessage> Head);

/// <summary>A message waiting in a mailbox.</summary>
/// <param name="MessageType">CLR type name of the message.</param>
/// <param name="Sender">Absolute path of the sender, or <c>&lt;deadLetters&gt;</c>.</param>
/// <param name="Payload">The message's <see cref="object.ToString"/>, as in the tap stream.</param>
public sealed record QueuedMessage(string MessageType, string Sender, string Payload);
