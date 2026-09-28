// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.RegularExpressions;

namespace Kuestenlogik.Bowire.Protocol.Akka;

/// <summary>
/// Which tapped messages one subscriber wants (#31): by recipient path, by
/// message type, with or without dead letters.
/// </summary>
/// <remarks>
/// <para>
/// Patterns take <c>*</c> for any run of characters and match the whole
/// value. A path pattern matches the full path
/// (<c>akka://Harbor/user/dock-*</c>) or the path without its address
/// (<c>/user/dock-*</c>); a type pattern matches the full type name
/// (<c>Harbor.Messages.*</c>) or the simple one (<c>PortCall*</c>). Several
/// patterns of one kind are alternatives; the two kinds must both match. An
/// empty list places no restriction.
/// </para>
/// <para>
/// The filter runs in <see cref="BowireAkkaExtension.Publish"/>, before the
/// subscriber's channel: what it rejects never takes a slot there, so a
/// narrow filter also keeps the drop-oldest channel from losing the messages
/// that were asked for.
/// </para>
/// </remarks>
public sealed class TapFilter
{
    private readonly Regex[] _paths;
    private readonly Regex[] _types;

    /// <summary>Messages only for actors matching one of these; empty for any.</summary>
    public IReadOnlyList<string> Paths { get; }

    /// <summary>Messages only of a type matching one of these; empty for any.</summary>
    public IReadOnlyList<string> MessageTypes { get; }

    /// <summary>Whether dead letters come through (they still have to match the patterns).</summary>
    public bool IncludeDeadLetters { get; }

    /// <summary>Build a filter; blank patterns are ignored.</summary>
    public TapFilter(IEnumerable<string>? paths = null, IEnumerable<string>? messageTypes = null, bool includeDeadLetters = true)
    {
        Paths = Clean(paths);
        MessageTypes = Clean(messageTypes);
        IncludeDeadLetters = includeDeadLetters;
        _paths = [.. Paths.Select(Compile)];
        _types = [.. MessageTypes.Select(Compile)];
    }

    /// <summary>True when the filter lets everything through.</summary>
    public bool IsEmpty => _paths.Length == 0 && _types.Length == 0 && IncludeDeadLetters;

    /// <summary>Whether <paramref name="message"/> passes.</summary>
    public bool Matches(TappedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.IsDeadLetter && !IncludeDeadLetters) return false;
        // A dead letter's recipient is the dead-letter actor; the actor it was
        // meant for is not in the envelope. So a path pattern keeps dead
        // letters out unless it names the dead-letter path itself.
        if (_paths.Length > 0 && !_paths.Any(p => p.IsMatch(message.Recipient) || p.IsMatch(WithoutAddress(message.Recipient))))
            return false;
        if (_types.Length > 0 && !_types.Any(t => t.IsMatch(message.MessageType) || t.IsMatch(SimpleName(message.MessageType))))
            return false;
        return true;
    }

    private static string[] Clean(IEnumerable<string>? patterns) =>
        patterns is null ? [] : [.. patterns.Where(p => !string.IsNullOrWhiteSpace(p)).Select(p => p.Trim()).Distinct(StringComparer.Ordinal)];

    private static Regex Compile(string pattern) =>
        new("^" + Regex.Escape(pattern).Replace("\\*", ".*", StringComparison.Ordinal) + "$",
            RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds(50));

    // akka://Harbor/user/dock-1 or akka.tcp://Harbor@host:port/user/dock-1 → /user/dock-1
    private static string WithoutAddress(string path)
    {
        var scheme = path.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0) return path;
        var slash = path.IndexOf('/', scheme + 3);
        return slash < 0 ? path : path[slash..];
    }

    private static string SimpleName(string typeName)
    {
        var cut = typeName.LastIndexOfAny(['.', '+']);
        return cut < 0 ? typeName : typeName[(cut + 1)..];
    }
}
