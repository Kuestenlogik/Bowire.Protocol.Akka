// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Akka.Actor;
using Akka.Serialization;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// How a tap subscriber wants each message as structured JSON (#30). Chosen
/// per subscriber in the MonitorMessages request.
/// </summary>
public enum PayloadFormat
{
    /// <summary>No structured payload — only the <see cref="object.ToString"/> rendering.</summary>
    None = 0,

    /// <summary>
    /// The most readable rendering that works: a JSON serializer the actor
    /// system binds to the type on purpose, protobuf's own JSON mapping for a
    /// protobuf message, the public properties, the instance fields when the
    /// properties show nothing, Akka's default JSON as the last resort.
    /// </summary>
    Auto = 1,

    /// <summary>The public properties, via System.Text.Json.</summary>
    Properties = 2,

    /// <summary>
    /// The instance fields, public and private, read by reflection — the
    /// object's actual state, also for a class that exposes none of it.
    /// </summary>
    Fields = 3,

    /// <summary>
    /// What the configured Akka serializer would put on the wire: its JSON
    /// as is, or — for a binary serializer such as protobuf or Hyperion — the
    /// serializer's name, the byte count and the bytes in base64.
    /// </summary>
    Akka = 4,
}

/// <summary>
/// A message as structured JSON for the tap (#30), so the workbench can show
/// its fields rather than a <see cref="object.ToString"/> line.
/// </summary>
/// <remarks>
/// <para>
/// The tap sees the message object before anything serializes it — inside
/// one process Akka never does — so no format has to be decoded from bytes:
/// protobuf and Hyperion bindings matter only for <see cref="PayloadFormat.Akka"/>,
/// which shows what they would send.
/// </para>
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

    private const int MaxDepth = 8;
    private const int MaxItems = 100;

    private static readonly JsonSerializerOptions PropertyOptions = new()
    {
        ReferenceHandler = ReferenceHandler.IgnoreCycles,
        MaxDepth = 32,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
        Converters = { new JsonStringEnumConverter() },
    };

    internal static JsonElement? Render(ExtendedActorSystem system, object? message, PayloadFormat format)
    {
        if (message is null || format == PayloadFormat.None) return null;
        try
        {
            return format switch
            {
                PayloadFormat.Properties => Properties(message),
                PayloadFormat.Fields => Fields(message),
                PayloadFormat.Akka => AkkaWire(system, message),
                _ => Auto(system, message),
            };
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return null;
        }
    }

    private static JsonElement? Auto(ExtendedActorSystem system, object message)
    {
        var configured = Try(() => system.Serialization.FindSerializerFor(message));
        if (configured is not null and not NewtonSoftJsonSerializer && Json(Try(() => configured.ToBinary(message))) is { } custom)
            return custom;
        if (Protobuf(message) is { } proto) return proto;
        var properties = TryJson(() => Properties(message));
        if (properties is { ValueKind: JsonValueKind.Object } p && !p.EnumerateObject().Any() && InstanceFields(message.GetType()).Any())
            return Fields(message);
        if (properties is not null) return properties;
        return configured is NewtonSoftJsonSerializer
            ? Json(Try(() => configured.ToBinary(message))) ?? Fields(message)
            : Fields(message);
    }

    private static JsonElement? Properties(object message) =>
        Json(JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), PropertyOptions));

    private static JsonElement? AkkaWire(ExtendedActorSystem system, object message)
    {
        var serializer = system.Serialization.FindSerializerFor(message);
        var bytes = serializer.ToBinary(message);
        if (Json(bytes) is { } json) return json;
        if (bytes.Length > MaxBytes) return null;
        return JsonSerializer.SerializeToElement(new
        {
            serializer = serializer.GetType().FullName,
            bytes = bytes.Length,
            base64 = Convert.ToBase64String(bytes),
        });
    }

    // ---- protobuf, without a reference to Google.Protobuf ----

    /// <summary>
    /// A protobuf message in protobuf's canonical JSON mapping — int64 as a
    /// string, enums by their proto name, well-known types as their JSON form.
    /// Found by reflection, so the plugin carries no protobuf dependency.
    /// </summary>
    private static JsonElement? Protobuf(object message)
    {
        var imessage = message.GetType().GetInterface("Google.Protobuf.IMessage");
        if (imessage is null) return null;
        var formatter = imessage.Assembly.GetType("Google.Protobuf.JsonFormatter");
        var instance = formatter?.GetProperty("Default", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
        var format = formatter?.GetMethod("Format", [imessage]);
        if (instance is null || format is null) return null;
        return Try(() => format.Invoke(instance, [message]) as string) is { } text
            ? Json(System.Text.Encoding.UTF8.GetBytes(text))
            : null;
    }

    // ---- fields ----

    private static IEnumerable<FieldInfo> InstanceFields(Type type)
    {
        for (var t = type; t is not null && t != typeof(object); t = t.BaseType)
        {
            foreach (var f in t.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                yield return f;
        }
    }

    private static JsonElement? Fields(object message)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteValue(writer, message, 0, new HashSet<object>(ReferenceEqualityComparer.Instance));
        }
        return buffer.Length <= MaxBytes ? Json(buffer.ToArray()) : null;
    }

    private static void WriteValue(Utf8JsonWriter w, object? value, int depth, HashSet<object> seen)
    {
        switch (value)
        {
            case null: w.WriteNullValue(); return;
            case string s: w.WriteStringValue(s); return;
            case bool b: w.WriteBooleanValue(b); return;
            case char c: w.WriteStringValue(c.ToString()); return;
            case Enum e: w.WriteStringValue(e.ToString()); return;
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                w.WriteNumberValue(Convert.ToDecimal(value, System.Globalization.CultureInfo.InvariantCulture)); return;
            case float f: WriteDouble(w, f); return;
            case double d: WriteDouble(w, d); return;
            case decimal m: w.WriteNumberValue(m); return;
            case DateTime or DateTimeOffset or TimeSpan or Guid or Uri or Type:
                w.WriteStringValue(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)); return;
            case Delegate or Pointer or IntPtr or UIntPtr:
                w.WriteStringValue($"<{value.GetType().Name}>"); return;
        }

        var type = value.GetType();
        if (depth >= MaxDepth) { w.WriteStringValue($"<{type.Name}>"); return; }
        if (!type.IsValueType && !seen.Add(value)) { w.WriteStringValue("<cycle>"); return; }
        try
        {
            if (value is IDictionary dict)
            {
                w.WriteStartObject();
                var n = 0;
                foreach (DictionaryEntry entry in dict)
                {
                    if (n++ >= MaxItems) break;
                    w.WritePropertyName(Convert.ToString(entry.Key, System.Globalization.CultureInfo.InvariantCulture) ?? "");
                    WriteValue(w, entry.Value, depth + 1, seen);
                }
                w.WriteEndObject();
                return;
            }
            if (value is IEnumerable list)
            {
                w.WriteStartArray();
                var n = 0;
                foreach (var item in list)
                {
                    if (n++ >= MaxItems) break;
                    WriteValue(w, item, depth + 1, seen);
                }
                w.WriteEndArray();
                return;
            }

            w.WriteStartObject();
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var field in InstanceFields(type))
            {
                var name = DisplayName(field);
                // A shadowed field of a base type keeps its type's name, so no
                // property is written twice.
                if (!names.Add(name)) name = $"{field.DeclaringType?.Name}.{name}";
                object? fieldValue;
                try { fieldValue = field.GetValue(value); }
                catch (Exception ex) when (ex is not OutOfMemoryException) { continue; }
                w.WritePropertyName(name);
                WriteValue(w, fieldValue, depth + 1, seen);
            }
            w.WriteEndObject();
        }
        finally
        {
            if (!type.IsValueType) seen.Remove(value);
        }
    }

    private static void WriteDouble(Utf8JsonWriter w, double d)
    {
        if (double.IsFinite(d)) w.WriteNumberValue(d);
        else w.WriteStringValue(d.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    // "<ShipId>k__BackingField" is the auto-property ShipId; "_shipId" stays as written.
    private static string DisplayName(FieldInfo field)
    {
        var name = field.Name;
        if (name.StartsWith('<') && field.IsDefined(typeof(CompilerGeneratedAttribute)))
        {
            var end = name.IndexOf('>', StringComparison.Ordinal);
            if (end > 1) return name[1..end];
        }
        return name;
    }

    // ---- helpers ----

    private static JsonElement? Json(byte[]? bytes)
    {
        if (bytes is null || bytes.Length == 0 || bytes.Length > MaxBytes) return null;
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

    private static T? Try<T>(Func<T?> f) where T : class
    {
        try { return f(); }
        catch (Exception ex) when (ex is not OutOfMemoryException) { return null; }
    }

    private static JsonElement? TryJson(Func<JsonElement?> f)
    {
        try { return f(); }
        catch (Exception ex) when (ex is not OutOfMemoryException) { return null; }
    }
}
