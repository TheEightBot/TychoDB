namespace TychoDB.UnitTests.Collision;

/// <summary>
/// Deliberately shares its short type name with
/// <c>TychoDB.UnitTests.IndexDdlTests.IndexTestModel</c> so that index-name
/// collisions across namespaces are covered by the index tests.
/// </summary>
public class IndexTestModel
{
    public string OtherProperty { get; set; }
}
