using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// Regression tests for serializer output integrity.
/// </summary>
[TestClass]
public class SerializerRegressionTests
{
    // Guards against the UTF-8 BOM regression: NewtonsoftJsonSerializer used to emit
    // a BOM (StreamWriter with Encoding.UTF8), which SQLite's json() rejects as
    // malformed on stricter/older builds (notably the SQLCipher bundle), breaking
    // every write on TychoDB.Encrypted.
    [TestMethod]
    public void Newtonsoft_SerializedBytes_DoNotStartWithUtf8Bom()
    {
        var serializer = new NewtonsoftJsonSerializer();

        var bytes = (byte[])serializer.Serialize(new TestClassA
        {
            StringProperty = "value",
            IntProperty = 1,
            TimestampMillis = 123,
        });

        bytes.Length.ShouldBeGreaterThanOrEqualTo(3);
        (bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
            .ShouldBeFalse("Serialized JSON must not begin with a UTF-8 BOM");
    }
}
