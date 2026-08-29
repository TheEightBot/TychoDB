using System;
using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;

namespace TychoDB;

/// <summary>
/// Lets an equality or set-membership filter on a type's id property be answered from the
/// indexed <c>Key</c> column instead of a <c>JSON_EXTRACT</c> scan of every row.
/// </summary>
/// <remarks>
/// <para>
/// The rewrite is only correct while every row of the type is keyed by that id property, which
/// Tycho does not guarantee in general: <c>WriteObjectsAsync(objs, keySelector, …)</c> takes a
/// key at the call site and may disagree with the registration. Two things together make it
/// safe here. Strict registration rejects a divergent write outright, so no row written through
/// this instance can break the invariant; and rows already in the database — written by an
/// earlier version, or outside strict mode — are checked once with
/// <see cref="Queries.SelectKeyDivergesFromIdProperty"/> before the rewrite is used for that
/// type. A single divergent row disables the rewrite for the type, and the ordinary predicate
/// is emitted instead, so the worst case is the performance that was there before.
/// </para>
/// <para>
/// The probe is a scan, so it is run lazily — only when a query that could benefit actually
/// arrives — and its verdict is cached for the lifetime of the connection.
/// </para>
/// </remarks>
internal sealed class KeyColumnRewrite
{
    private readonly ConcurrentDictionary<string, bool> _usableByTypeName = new(StringComparer.Ordinal);

    public KeyColumnRewrite(string resolvedIdPath)
    {
        ResolvedIdPath = resolvedIdPath;
    }

    /// <summary>
    /// Gets the id property's JSON path, already rendered through the serializer's member names
    /// so it can be compared directly against a filter's resolved path.
    /// </summary>
    public string ResolvedIdPath { get; }

    /// <summary>
    /// Returns this rewrite when the stored keys for <paramref name="fullTypeName"/> provably
    /// match the id property, and <see langword="null"/> when they do not — in which case the
    /// caller emits the ordinary JSON path predicate.
    /// </summary>
    public KeyColumnRewrite? VerifiedFor(SqliteConnection connection, string fullTypeName)
    {
        var usable =
            _usableByTypeName.GetOrAdd(
                fullTypeName,
                static (name, state) => !state.Self.HasDivergentRow(state.Connection, name),
                (Self: this, Connection: connection));

        return usable ? this : null;
    }

    private bool HasDivergentRow(SqliteConnection connection, string fullTypeName)
    {
        using var command = connection.CreateCommand();

#pragma warning disable CA2100 // The path is rendered from a property expression and validated by QueryPropertyPath.
        command.CommandText = Queries.SelectKeyDivergesFromIdProperty(ResolvedIdPath);
#pragma warning restore CA2100
        command.Parameters.Add("$fullTypeName", SqliteType.Text).Value = fullTypeName;

        using var reader = command.ExecuteReader();
        return reader.Read();
    }
}
