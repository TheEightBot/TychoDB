namespace TychoDB;

/// <summary>
/// Describes an index Tycho created, as recorded in its index metadata table.
/// </summary>
/// <param name="IndexName">The logical name supplied to CreateIndex.</param>
/// <param name="FullTypeName">
/// The indexed type. This is the fully-qualified CLR type name for indexes created
/// through the generic overloads, or the short type name supplied to the manual
/// string overload.
/// </param>
/// <param name="PhysicalName">The name of the index in the SQLite schema.</param>
/// <param name="Definition">The CREATE INDEX statement used to build it.</param>
public readonly record struct TychoIndexInfo(
    string IndexName,
    string FullTypeName,
    string PhysicalName,
    string Definition);
