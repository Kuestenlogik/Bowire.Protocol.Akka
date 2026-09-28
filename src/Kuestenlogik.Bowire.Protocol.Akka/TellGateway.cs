// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using System.Text.RegularExpressions;
using Akka.Actor;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// What the host allows the workbench to send into its actor system (#32):
/// which actors, and which message types. Nothing is allowed until the host
/// enables it with <see cref="BowireAkkaExtension.EnableTell"/> or the
/// <c>bowire.akka.tell</c> HOCON section.
/// </summary>
/// <remarks>
/// <para>
/// A path pattern takes <c>*</c> for any run of characters and is matched
/// against the actor's path from the root (<c>/user/dock-*</c>). A message
/// type is an exact CLR type: the workbench picks one of these by name, and
/// the JSON it sends is deserialized into exactly that type — never into a
/// type the request names, so a <c>$type</c> in the body cannot reach
/// anything the host did not list.
/// </para>
/// </remarks>
public sealed class TellPolicy
{
    private readonly Regex[] _paths;

    /// <summary>Path patterns of the actors that may be told something.</summary>
    public IReadOnlyList<string> Paths { get; }

    /// <summary>The message types that may be sent, by full name.</summary>
    public IReadOnlyDictionary<string, Type> MessageTypes { get; }

    /// <summary>Allow <paramref name="messageTypes"/> to be sent to actors matching <paramref name="paths"/>.</summary>
    /// <exception cref="ArgumentException">No path or no type: a policy that allows nothing is a mistake, not a policy.</exception>
    public TellPolicy(IEnumerable<string> paths, IEnumerable<Type> messageTypes)
    {
        ArgumentNullException.ThrowIfNull(paths);
        ArgumentNullException.ThrowIfNull(messageTypes);
        Paths = [.. paths.Where(p => !string.IsNullOrWhiteSpace(p)).Select(p => p.Trim()).Distinct(StringComparer.Ordinal)];
        MessageTypes = messageTypes.Distinct().ToDictionary(t => t.FullName ?? t.Name, StringComparer.Ordinal);
        if (Paths.Count == 0) throw new ArgumentException("A tell policy needs at least one actor path pattern.", nameof(paths));
        if (MessageTypes.Count == 0) throw new ArgumentException("A tell policy needs at least one message type.", nameof(messageTypes));
        _paths = [.. Paths.Select(p => new Regex(
            "^" + Regex.Escape(p).Replace("\\*", ".*", StringComparison.Ordinal) + "$",
            RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds(50)))];
    }

    internal bool AllowsPath(string pathFromRoot) => _paths.Any(p => p.IsMatch(pathFromRoot));
}

/// <summary>One accepted tell: where it went and what it was.</summary>
internal sealed record TellTarget(ActorSelection Selection, string Path, object Message);

/// <summary>
/// Checks a tell request against the host's <see cref="TellPolicy"/> and
/// turns it into a message (#32). Everything a request carries is data; the
/// policy is the only thing that decides what may be sent where.
/// </summary>
internal static class TellGateway
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// The actor and message a request asks for, or the reason it is refused.
    /// </summary>
    internal static (TellTarget? Target, string? Refusal) Resolve(ActorSystem system, TellPolicy? policy, string json)
    {
        if (policy is null) return (null, "Sending messages is not enabled on this actor system.");

        string? path, typeName;
        JsonElement message;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return (null, "A tell request is a JSON object with path, messageType and message.");
            path = root.TryGetProperty("path", out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;
            typeName = root.TryGetProperty("messageType", out var t) && t.ValueKind == JsonValueKind.String ? t.GetString() : null;
            message = root.TryGetProperty("message", out var m) ? m.Clone() : default;
        }
        catch (JsonException ex)
        {
            return (null, $"The request is not JSON: {ex.Message}");
        }

        if (string.IsNullOrWhiteSpace(path)) return (null, "The request names no actor path.");
        var local = LocalPath(system, path.Trim());
        if (local is null)
            return (null, $"'{path}' is not a concrete path in this actor system: no wildcards, no '..', no other address.");
        if (!policy.AllowsPath(local)) return (null, $"'{local}' is not an actor this system allows messages to.");

        if (string.IsNullOrWhiteSpace(typeName) || !policy.MessageTypes.TryGetValue(typeName.Trim(), out var type))
            return (null, $"'{typeName}' is not a message type this system allows.");

        object? value;
        try
        {
            value = message.ValueKind == JsonValueKind.Undefined
                ? JsonSerializer.Deserialize("{}", type, Options)
                : message.Deserialize(type, Options);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException or InvalidOperationException)
        {
            return (null, $"The message does not make a {type.FullName}: {ex.Message}");
        }
        if (value is null) return (null, $"The message does not make a {type.FullName}.");

        return (new TellTarget(system.ActorSelection(local), local, value), null);
    }

    /// <summary>
    /// The path from the root for a concrete actor of this system, or null.
    /// </summary>
    /// <remarks>
    /// An actor selection is more than a path: <c>*</c> and <c>?</c> send to
    /// every match, <c>..</c> climbs to the parent — <c>/user/dock-1/../admin</c>
    /// would pass a <c>/user/dock-*</c> policy and reach <c>/user/admin</c> —
    /// and another address sends over the network. None of that is a tell
    /// the policy could have meant, so none of it is accepted.
    /// </remarks>
    internal static string? LocalPath(ActorSystem system, string path)
    {
        var own = $"akka://{system.Name}";
        if (path.Contains("://", StringComparison.Ordinal))
        {
            if (!path.StartsWith(own + "/", StringComparison.Ordinal)) return null;
            path = path[own.Length..];
        }
        if (!path.StartsWith('/')) return null;
        var segments = path.Split('/');
        foreach (var segment in segments.Skip(1))
        {
            if (segment.Length == 0 || segment is "." or "..") return null;
            if (segment.IndexOfAny(['*', '?', '[', ']', '#']) >= 0) return null;
        }
        return path;
    }
}
