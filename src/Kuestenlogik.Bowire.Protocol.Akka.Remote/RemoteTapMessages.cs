// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Akka.Remote;

// What goes between the bowire CLI and the relay in the host (#27). Plain
// records of strings and numbers: they cross Akka remoting with whatever
// serializer the systems use for object, Akka's default JSON included.

/// <summary>Ask the relay whether it is there.</summary>
public sealed record RemoteTapPing(string ActorSystem);

/// <summary>The relay's answer to <see cref="RemoteTapPing"/>.</summary>
/// <param name="ActorSystem">Name of the actor system the relay taps.</param>
public sealed record RemoteTapPong(string ActorSystem);

/// <summary>Start a subscription, or renew one: the relay drops a subscription it has not heard of for the lease time.</summary>
/// <param name="SubscriptionId">Chosen by the client; the same id renews.</param>
/// <param name="Paths">Path patterns, as for the MonitorMessages filter.</param>
/// <param name="MessageTypes">Type patterns, as for the MonitorMessages filter.</param>
/// <param name="IncludeDeadLetters">Whether dead letters come through.</param>
/// <param name="PayloadFormat">A <see cref="Akka.PayloadFormat"/>, by number.</param>
public sealed record RemoteTapSubscribe(
    string SubscriptionId,
    IReadOnlyList<string> Paths,
    IReadOnlyList<string> MessageTypes,
    bool IncludeDeadLetters,
    int PayloadFormat);

/// <summary>End a subscription.</summary>
public sealed record RemoteTapUnsubscribe(string SubscriptionId);

/// <summary>The relay's confirmation of a subscription, and its heartbeat while nothing happens.</summary>
public sealed record RemoteTapAlive(string SubscriptionId);

/// <summary>One tapped message, as it crosses the wire.</summary>
/// <remarks>The structured payload travels as JSON text; the receiving side parses it back.</remarks>
public sealed record RemoteTapped(
    string SubscriptionId,
    string Recipient,
    string Sender,
    string MessageType,
    string Payload,
    DateTime Timestamp,
    bool IsDeadLetter,
    string? PayloadJson);
