// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using System.Text.Json.Serialization;
using Akka.Actor;
using Akka.Serialization;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// A message as structured JSON for the tap (#30), so the workbench can show
/// its fields rather than a <see cref="object.ToString"/> line.
/// </summary>
/// <remarks>
/// <para>
/// In this order:
/// </para>
/// <list type="number">
///   <item>A serializer the actor system has bound to the message's type on
///   purpose — anything but Akka's default — whose output is JSON: that is
///   the format the application chose for the type, so it is shown.</item>
///   <item>System.Text.Json over the public properties: numbers as numbers,
///   strings as strings, what a field view needs.</item>
///   <item>Akka's default serializer, when System.Text.Json cannot handle the
///   type. Its JSON is a type-preserving wire format
///   (<c>{"$type": ..., "ShipId": {"$": "I17"}}</c>), which is why it is not
///   the first choice even though it is what the system would send.</item>
/// </list>
/// <para>
/// Anything that fails gives null, never an exception: this runs inside the
/// sender's <c>Tell</c>, and diagnostics must not break delivery. A payload
/// above <see cref="MaxBytes"/> is left out too — the string rendering is
/// still there.
/// </para>
/// </remarks>
internal static class PayloadRenderer
{
    /// <summary>Largest structured payload the tap carries.</summary>
    internal const int MaxBytes = 256 * 1024;

    private static readonly JsonSerializerOptions Fallback = new()
    {
        ReferenceHandler = ReferenceHandler.IgnoreCycles,
        MaxDepth = 32,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
        Converters = { new JsonStringEnumConverter() },
    };

    internal static JsonElement? Render(ExtendedActorSystem system, object? message)
    {
        if (message is null) return null;

        Serializer? configured = null;
        try { configured = system.Serialization.FindSerializerFor(message); }
        catch (Exception ex) when (ex is not OutOfMemoryException) { /* no binding; carry on */ }

        if (configured is not null and not NewtonSoftJsonSerializer && ViaAkka(configured, message) is { } custom)
            return custom;

        try
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), Fallback);
            return bytes.Length <= MaxBytes ? TryParse(bytes) : null;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // System.Text.Json cannot handle the type; Akka's own JSON may.
        }

        return configured is NewtonSoftJsonSerializer ? ViaAkka(configured, message) : null;
    }

    private static JsonElement? ViaAkka(Serializer serializer, object message)
    {
        try
        {
            var bytes = serializer.ToBinary(message);
            return bytes.Length <= MaxBytes ? TryParse(bytes) : null;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return null;
        }
    }

    private static JsonElement? TryParse(byte[] bytes)
    {
        try
        {
            using var doc = JsonDocument.Parse(bytes);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
