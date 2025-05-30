using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TychoDB;

[assembly: Parallelize(Scope = ExecutionScope.ClassLevel)]

namespace TychoDB.UnitTests;

[TestClass]
public class TychoDbTests
{
    private static readonly IJsonSerializer _systemTextJsonSerializer =
        new SystemTextJsonSerializer(
            jsonTypeSerializers:
                new Dictionary<Type, JsonTypeInfo>
                {
                    [typeof(TestClassA)] = TestJsonContext.Default.TestClassA,
                    [typeof(List<TestClassA>)] = TestJsonContext.Default.ListTestClassA,
                    [typeof(TestClassB)] = TestJsonContext.Default.TestClassB,
                    [typeof(TestClassC)] = TestJsonContext.Default.TestClassC,
                    [typeof(TestClassD)] = TestJsonContext.Default.TestClassD,
                    [typeof(TestClassE)] = TestJsonContext.Default.TestClassE,
                    [typeof(TestClassF)] = TestJsonContext.Default.TestClassF,
                });

    private static readonly IJsonSerializer _newtonsoftJsonSerializer = new NewtonsoftJsonSerializer();

    public static IEnumerable<object[]> JsonSerializers
    {
        get
        {
            yield return new[] { _systemTextJsonSerializer };
            yield return new[] { _newtonsoftJsonSerializer };
        }
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertObject_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var result = await db.WriteObjectAsync(testObj, x => x.StringProperty);

        result.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_RegisterAndInsertObject_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistrationWithCustomKeySelector<TestClassA>(x => $"{x.StringProperty}_{x.IntProperty}_TEST")
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var result = await db.WriteObjectAsync(testObj);

        result.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    [ExpectedException(typeof(TychoException))]
    public async Task TychoDb_InsertObjectWithoutRequiredRegistration_ShouldNotBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var result = await db.WriteObjectAsync(testObj);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertObjectWithRequiredRegistration_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .AddTypeRegistrationWithCustomKeySelector<TestClassA>(x => x.StringProperty + "_TEST")
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var result = await db.WriteObjectAsync(testObj);

        result.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    [ExpectedException(typeof(TychoException))]
    public async Task TychoDb_InsertObjectWithRequiredRegistrationAndGenericTypeRegistrationAndNoKeySelector_ShouldThrowError(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .AddTypeRegistration<TestClassA>()
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var result = await db.WriteObjectAsync(testObj);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public void TychoDb_RegisterTypeAndCompareIds_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .AddTypeRegistration<TestClassA, int>(x => x.IntProperty)
                .Connect();

        var testObj1 =
            new TestClassA
            {
                IntProperty = 1984,
            };

        var testObj2 =
            new TestClassA
            {
                IntProperty = 1984,
            };

        var successful = db.CompareIdsFor<TestClassA>(testObj1, testObj2);

        Assert.IsTrue(successful);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public void TychoDb_RegisterTypeAndCompareObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .AddTypeRegistration<TestClassA, int>(x => x.IntProperty)
                .Connect();

        var testObj1 =
            new TestClassA
            {
                IntProperty = 1984,
            };

        var testObj2 =
            new TestClassA
            {
                IntProperty = 1984,
            };

        var successful = db.CompareIdsFor<TestClassA>(testObj1, testObj2);

        Assert.IsTrue(successful);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public void TychoDb_RegisterTypeAndCompareObjectsWithDifferentIds_ShouldNotBeEqual(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .AddTypeRegistration<TestClassA, int>(x => x.IntProperty)
                .Connect();

        var testObj1 =
            new TestClassA
            {
                IntProperty = 1984,
            };

        var testObj2 =
            new TestClassA
            {
                IntProperty = 1985,
            };

        var successful = db.CompareIdsFor<TestClassA>(testObj1, testObj2);

        Assert.IsFalse(successful);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertObjectWithRequiredRegistrationAndGenericTypeRegistrationAndKeySelector_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer, true)
                .AddTypeRegistration<TestClassA>()
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var result = await db.WriteObjectAsync(testObj, x => x.StringProperty);

        result.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_RegisterPatientAndInsertObject_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistration<Patient, long>(x => x.PatientId)
                .Connect();

        var testObj =
            new Patient
            {
                PatientId = 12345,
                FirstName = "TEST",
                LastName = "PATIENT",
            };

        var result = await db.WriteObjectAsync(testObj);

        result.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_RegisterPatientAndInsertObjectAndQueryByIsDirty_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistration<Patient, long>(x => x.PatientId)
                .Connect();

        var testObj =
            new Patient
            {
                PatientId = 12345,
                FirstName = "TEST",
                LastName = "PATIENT",
                IsDirty = true,
            };

        await db.WriteObjectAsync(testObj);

        var result =
            await db.ReadObjectsAsync<Patient>(
                    filter: FilterBuilder<Patient>
                        .Create()
                        .Filter(FilterType.Equals, x => x.IsDirty, true));

        result.Should().NotBeNullOrEmpty();
        result.Should().HaveCount(1);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertAndReadManyObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        var successWrites = 0;
        var successReads = 0;

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var tasks =
            Enumerable
                .Range(100, 1000)
                .Select(
                    async i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        var resultWrite = await db.WriteObjectAsync(testObj, x => x.StringProperty).ConfigureAwait(false);

                        if (resultWrite)
                        {
                            Interlocked.Increment(ref successWrites);
                        }

                        var resultRead = await db.ReadObjectAsync<TestClassA>(testObj.StringProperty).ConfigureAwait(false);

                        if (resultRead != null)
                        {
                            Interlocked.Increment(ref successReads);
                        }
                    })
                .ToList();

        await Task.WhenAll(tasks).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        successWrites.Should().Be(expected);
        successReads.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertManyObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var resultWrite = await db.WriteObjectsAsync(testObjs, x => x.StringProperty).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        resultWrite.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertManyObjectsThenUpdate_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        await db.WriteObjectsAsync(testObjs, x => x.StringProperty).ConfigureAwait(false);

        var resultWrite = await db.WriteObjectsAsync(testObjs, x => x.StringProperty).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        resultWrite.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertManyObjectsWithNesting_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var rng = new Random();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testClassDs =
                            Enumerable
                                .Range(10, 100)
                                .Select(
                                    i =>
                                    {
                                        var testObj =
                                            new TestClassD
                                            {
                                                DoubleProperty = rng.NextDouble(),
                                                FloatProperty = (float)rng.NextDouble(),
                                            };

                                        return testObj;
                                    })
                                .ToList();

                        var testObj =
                            new TestClassE
                            {
                                TestClassId = Guid.NewGuid(),
                                Values = testClassDs,
                            };

                        return testObj;
                    })
                .ToList();

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var resultWrite = await db.WriteObjectsAsync(testObjs, x => x.TestClassId.ToString()).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        resultWrite.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertManyObjectsWithNestingAndFilterUsingGreaterThan_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 500;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var rng = new Random();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testClassDs =
                            Enumerable
                                .Range(10, 10)
                                .Select(
                                    ii =>
                                    {
                                        var testObj =
                                            new TestClassD
                                            {
                                                DoubleProperty = rng.NextDouble(),
                                                FloatProperty = i % 2 == 0 ? 251 : 0,
                                            };

                                        return testObj;
                                    })
                                .ToList();

                        var testObj =
                            new TestClassE
                            {
                                TestClassId = Guid.NewGuid(),
                                Values = testClassDs,
                            };

                        return testObj;
                    })
                .ToList();

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var resultWrite = await db.WriteObjectsAsync(testObjs, x => x.TestClassId.ToString()).ConfigureAwait(false);

        var resultRead =
            await db
                .ReadObjectsAsync<TestClassE>(
                    filter: FilterBuilder<TestClassE>
                        .Create()
                        .Filter(FilterType.GreaterThan, x => x.Values, x => x.FloatProperty, 250d));

        var resultReadCount = resultRead.Count();

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        resultReadCount.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.StringProperty).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs = await db.ReadObjectsAsync<TestClassA>().ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(testObjs.Count);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyObjectsWithPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "partition_name";

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        // Insert without partition, then with
        await db.WriteObjectsAsync(testObjs.Take(100), x => x.StringProperty).ConfigureAwait(false);
        await db.WriteObjectsAsync(testObjs, x => x.StringProperty, partition).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs = await db.ReadObjectsAsync<TestClassA>(partition: partition).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_CountManyObjectsWithPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "partition_name";

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        // Insert without partition, then with
        await db.WriteObjectsAsync(testObjs.Take(100), x => x.StringProperty).ConfigureAwait(false);
        await db.WriteObjectsAsync(testObjs, x => x.StringProperty, partition).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objsCount = await db.CountObjectsAsync<TestClassA>(partition: partition).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objsCount.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyGenericObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectAsync(testObjs, x => x.GetHashCode()).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var obj = await db.ReadObjectAsync<List<TestClassA>>(testObjs.GetHashCode()).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        obj.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_CountManyGenericObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectAsync(testObjs, x => x.GetHashCode()).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var exists = await db.ObjectExistsAsync<List<TestClassA>>(testObjs.GetHashCode()).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        exists.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyInnerObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassF
                            {
                                TestClassId = Guid.NewGuid(),
                                Value =
                                    new TestClassD
                                    {
                                        DoubleProperty = 1234d,
                                        FloatProperty = 4567f,
                                    },
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs = await db.ReadObjectsAsync<TestClassF, TestClassD>(x => x.Value).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyInnerObjectsWithKeys_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var id = Guid.NewGuid();
                        var testObj =
                            new TestClassF
                            {
                                TestClassId = id,
                                Value =
                                    new TestClassD
                                    {
                                        DoubleProperty = 1234d,
                                        FloatProperty = 4567f,
                                        StringProperty = id.ToString(),
                                    },
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs = await db.ReadObjectsWithKeysAsync<TestClassF, TestClassD>(x => x.Value).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
        objs.All(x => Guid.Parse(x.Key) == Guid.Parse(x.InnerObject.StringProperty)).Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyInnerObjectProperty_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassF
                            {
                                TestClassId = Guid.NewGuid(),
                                Value =
                                    new TestClassD
                                    {
                                        DoubleProperty = 1234d,
                                        FloatProperty = 4567f,
                                    },
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs = await db.ReadObjectsAsync<TestClassF, double>(x => x.Value.DoubleProperty).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyInnerObjectsWithLessThanFilter_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 500;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(0, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassF
                            {
                                TestClassId = Guid.NewGuid(),
                                Value =
                                    new TestClassD
                                    {
                                        DoubleProperty = i,
                                        FloatProperty = 4567f,
                                        ValueC =
                                            new TestClassC
                                            {
                                                IntProperty = i * 2,
                                            },
                                    },
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs =
            await db
                .ReadObjectsAsync<TestClassF, TestClassC>(
                    x => x.Value.ValueC,
                    filter: FilterBuilder<TestClassF>
                        .Create()
                        .Filter(FilterType.GreaterThan, x => x.Value.DoubleProperty, 250d)
                        .And()
                        .Filter(FilterType.LessThanOrEqualTo, x => x.Value.DoubleProperty, 750d));

        var count = objs.Count();

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        count.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_ReadManyInnerObjectsWithLessThanFilterAndIndex_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 750;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect()
                .CreateIndex<TestClassF>(x => x.Value.ValueC.IntProperty, "ValueCInt");

        var testObjs =
            Enumerable
                .Range(0, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassF
                            {
                                TestClassId = Guid.NewGuid(),
                                Value =
                                    new TestClassD
                                    {
                                        DoubleProperty = i,
                                        FloatProperty = 4567f,
                                        ValueC =
                                            new TestClassC
                                            {
                                                IntProperty = i,
                                            },
                                    },
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs =
            await db
                .ReadObjectsAsync<TestClassF, TestClassC>(
                    x => x.Value.ValueC,
                    filter: FilterBuilder<TestClassF>
                        .Create()
                        .Filter(FilterType.GreaterThanOrEqualTo, x => x.Value.ValueC.IntProperty, 250d));

        var count = objs.Count();

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        count.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertObjectAndQuery_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => x.StringProperty);
        var readResult = await db.ReadObjectAsync<TestClassA>(testObj.StringProperty);

        writeResult.Should().BeTrue();
        readResult.Should().NotBeNull();
        readResult.StringProperty.Should().Be(testObj.StringProperty);
        readResult.IntProperty.Should().Be(testObj.IntProperty);
        readResult.TimestampMillis.Should().Be(testObj.TimestampMillis);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryUsingContains_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.GetHashCode().ToString()).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs =
            await db
                .ReadObjectsAsync<TestClassA>(
                    filter: FilterBuilder<TestClassA>
                        .Create()
                        .Filter(FilterType.Contains, x => x.StringProperty, " String "))
                .ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryUsingEqualsOnAGuid_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = Guid.NewGuid();

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassF
            {
                TestClassId = expected,
            };

        await db.WriteObjectAsync(testObj, x => x.TestClassId).ConfigureAwait(false);

        var obj =
            await db
                .ReadObjectAsync<TestClassF>(
                    filter: FilterBuilder<TestClassF>
                        .Create()
                        .Filter(FilterType.Equals, x => x.TestClassId, expected))
                .ConfigureAwait(false);

        obj.Should().NotBeNull();
        obj.TestClassId.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryFirstUsingEqualsOnAGuid_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = Guid.NewGuid();

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassF
            {
                TestClassId = expected,
            };

        await db.WriteObjectAsync(testObj, x => x.TestClassId).ConfigureAwait(false);

        var obj =
            await db
                .ReadFirstObjectAsync<TestClassF>(
                    filter: FilterBuilder<TestClassF>
                        .Create()
                        .Filter(FilterType.Equals, x => x.TestClassId, expected))
                .ConfigureAwait(false);

        obj.Should().NotBeNull();
        obj.TestClassId.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryInnerObjectUsingEquals_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1;

        var doubleProperty = 1234.0d;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassF
            {
                TestClassId = Guid.NewGuid(),
                Value =
                    new TestClassD
                    {
                        DoubleProperty = doubleProperty,
                        FloatProperty = 5678f,
                    },
            };

        await db.WriteObjectAsync(testObj, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs =
            await db
                .ReadObjectsAsync<TestClassF>(
                    filter: FilterBuilder<TestClassF>
                        .Create()
                        .Filter(FilterType.Equals, x => x.Value.DoubleProperty, doubleProperty))
                .ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryInnerObjectCheckIsNull_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = 1;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassF
            {
                TestClassId = Guid.NewGuid(),
                Value = null,
            };

        await db.WriteObjectAsync(testObj, x => x.TestClassId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs =
            await db
                .ReadObjectsAsync<TestClassF>(
                    filter: FilterBuilder<TestClassF>
                        .Create()
                        .Filter(FilterType.Equals, x => x.Value, null))
                .ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryInnerObjectUsingEqualsWithDateTime_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var writeSuccessful = false;
        var expected = 1;
        var dobValue = DateTime.Now;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new Patient
            {
                PatientId = 12345,
                DOB = dobValue,
            };

        writeSuccessful = await db.WriteObjectAsync(testObj, x => x.PatientId).ConfigureAwait(false);

        writeSuccessful.Should().BeTrue();

        var objs =
            await db
                .ReadObjectsAsync<Patient>(
                    filter: FilterBuilder<Patient>
                        .Create()
                        .Filter(FilterType.Equals, x => x.DOB, testObj.DOB))
                .ConfigureAwait(false);

        objs.Count().Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_QueryInnerObjectUsingSortWithStringAndLong_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        Console.WriteLine($"Serializer: {jsonSerializer}");

        var expectedFirstId = 12L;
        var expectedLastId = 11L;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(1, 22)
                .Select(
                    i =>
                    {
                        var testObj =
                            new Patient
                            {
                                PatientId = i,
                                MRN = i < 12 ? "11111" : "99999",
                                DOB = DateTime.Now.AddDays(i),
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.PatientId).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objs =
            await db
                .ReadObjectsAsync<Patient>(
                    sort: SortBuilder<Patient>
                        .Create()
                        .OrderBy(SortDirection.Descending, x => x.MRN)
                        .OrderBy(SortDirection.Ascending, x => x.PatientId))
                .ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objs.First().PatientId.Should().Be(expectedFirstId);
        objs.Last().PatientId.Should().Be(expectedLastId);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_CreateDataIndex_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistration<TestClassD>()
                .Connect();

        var successful = await db.CreateIndexAsync<TestClassD>(x => x.DoubleProperty, "double_index");

        successful.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_CreateDataIndexWithGeneric_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistration<TestClassG<object>>()
                .Connect();

        var successful = await db.CreateIndexAsync<TestClassG<object>>(x => x.Id, "id_index");

        successful.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public void TychoDb_CreateDataIndexWithGenericAndMultipleIndexes_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistration<TestClassG<object>, Guid>(x => x.Id)
                .Connect()
                .CreateIndex<TestClassG<object>>(x => x.Id, "id1")
                .CreateIndex<TestClassG<object>>(x => x.Id, "id2")
                .CreateIndex<TestClassG<object>>(x => x.Id, "id3");
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_CreateDataIndexWithMultipleProperties_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expected = true;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .AddTypeRegistration<TestClassB>()
                .Connect();

        var successful =
            await db.CreateIndexAsync<TestClassB>(
                new Expression<Func<TestClassB, object>>[]
                {
                    x => x.StringProperty,
                    x => x.DoubleProperty,
                },
                "string_double_index");

        successful.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_DeleteObject_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObj =
            new TestClassA
            {
                StringProperty = "Test String",
                IntProperty = 1984,
                TimestampMillis = 123451234,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => x.StringProperty);
        var deleteResult = await db.DeleteObjectAsync<TestClassA>(testObj.StringProperty);

        writeResult.Should().BeTrue();
        deleteResult.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_DeleteManyObjects_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        var expectedCount = 1000;

        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var testObjs =
            Enumerable
                .Range(100, 1000)
                .Select(
                    i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                IntProperty = i,
                                TimestampMillis = 123451234,
                            };

                        return testObj;
                    })
                .ToList();

        await db.WriteObjectsAsync(testObjs, x => x.StringProperty).ConfigureAwait(false);

        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        var objsDeleted = await db.DeleteObjectsAsync<TestClassA>().ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        objsDeleted.Should().Be(expectedCount);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertBlob_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var textExample = "This is a test message";
        using var stream = new MemoryStream();
        using var writer = new StreamWriter(stream);
        writer.Write(textExample);
        writer.Flush();
        stream.Position = 0;

        var result = await db.WriteBlobAsync(stream, "Test");

        result.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertBlobAndQuery_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var textExample = "This is a test message";
        var key = "Test";

        using var stream = new MemoryStream();
        using var writer = new StreamWriter(stream);
        writer.Write(textExample);
        writer.Flush();
        stream.Position = 0;

        var insertResult = await db.WriteBlobAsync(stream, key);
        using var queryResult = await db.ReadBlobAsync(key);
        using var resultReader = new StreamReader(queryResult);
        var streamContents = await resultReader.ReadToEndAsync();

        insertResult.Should().BeTrue();
        streamContents.Should().BeEquivalentTo(textExample);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertBlobAndDelete_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var textExample = "This is a test message";
        var key = "Test";

        using var stream = new MemoryStream();
        using var writer = new StreamWriter(stream);
        writer.Write(textExample);
        writer.Flush();
        stream.Position = 0;

        var insertResult = await db.WriteBlobAsync(stream, key);
        var deleteResult = await db.DeleteBlobAsync(key);

        insertResult.Should().BeTrue();
        deleteResult.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertBlobAndCheckExists_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var textExample = "This is a test message";
        var key = "Test";

        using var stream = new MemoryStream();
        using var writer = new StreamWriter(stream);
        writer.Write(textExample);
        writer.Flush();
        stream.Position = 0;

        var insertResult = await db.WriteBlobAsync(stream, key);
        var existsResult = await db.BlobExistsAsync(key);

        insertResult.Should().BeTrue();
        existsResult.Should().BeTrue();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertManyBlobsAndDeleteManyBlobs_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var expected = 5;

        var textExample = "This is a test message";
        var partition = "partition";

        for (int i = 0; i < expected; i++)
        {
            using var stream = new MemoryStream();
            using var writer = new StreamWriter(stream);
            writer.Write(textExample);
            writer.Flush();
            stream.Position = 0;

            await db.WriteBlobAsync(stream, i.ToString(), partition);
        }

        var deleteResult = await db.DeleteBlobsAsync(partition);

        deleteResult.Successful.Should().BeTrue();
        deleteResult.Count.Should().Be(expected);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertMultipleObjectsWithSameKey_ShouldBeAbleToFindBoth(IJsonSerializer jsonSerializer)
    {
        using var db = BuildDatabaseConnection(jsonSerializer).Connect();

        var key = "key";

        var classAIntProperty = 1984;
        var classBDoubleProperty = 1999d;

        var testObjA =
            new TestClassA
            {
                StringProperty = key,
                IntProperty = classAIntProperty,
                TimestampMillis = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
            };

        var testObjB =
            new TestClassB
            {
                StringProperty = key,
                DoubleProperty = classBDoubleProperty,
            };

        await db.WriteObjectAsync(testObjA, x => x.StringProperty);
        await db.WriteObjectAsync(testObjB, x => x.StringProperty);

        var readA = await db.ReadObjectAsync<TestClassA>(key);
        var readB = await db.ReadObjectAsync<TestClassB>(key);

        readA.IntProperty.Should().Be(classAIntProperty);
        readB.DoubleProperty.Should().Be(classBDoubleProperty);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_InsertMultipleObjectsWithSameKeyAndDifferentPartition_ShouldBeAbleToFindBoth(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect()
                .AddTypeRegistration<TestClassA, string>(x => x.StringProperty);

        var key = "key";

        var partition1 = "partition_1";
        var partition2 = "partition_2";

        var obj1IntProperty = 1984;
        var obj2IntProperty = 1999;

        var testObj1 =
            new TestClassA
            {
                StringProperty = key,
                IntProperty = obj1IntProperty,
            };

        var testObj2 =
            new TestClassA
            {
                StringProperty = key,
                IntProperty = obj2IntProperty,
            };

        await db.WriteObjectsAsync(new[] { testObj1 }, partition1);
        await db.WriteObjectsAsync(new[] { testObj2 }, partition2);

        var readA = await db.ReadObjectAsync<TestClassA>(key, partition1);
        var readB = await db.ReadObjectAsync<TestClassA>(key, partition2);

        readA.IntProperty.Should().Be(obj1IntProperty);
        readB.IntProperty.Should().Be(obj2IntProperty);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public void TychoDb_ShouldShrinkMemory_IsSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        db.Cleanup();
    }

    public static Tycho BuildDatabaseConnection(IJsonSerializer jsonSerializer, bool requireTypeRegistration = false)
    {
#if ENCRYPTED
        return new Tycho(Path.GetTempPath(), jsonSerializer, $"{Guid.NewGuid()}_cache_enc.db", "Password", rebuildCache: true, requireTypeRegistration: requireTypeRegistration);
#else

        return new Tycho(Path.GetTempPath(), jsonSerializer, dbName: $"{Guid.NewGuid()}.db", rebuildCache: true, requireTypeRegistration: requireTypeRegistration);
#endif
    }
}

public class TestClassA
{
    public string StringProperty { get; set; }

    public int IntProperty { get; set; }

    public long TimestampMillis { get; set; }
}

public class TestClassB
{
    public string StringProperty { get; set; }

    public double DoubleProperty { get; set; }
}

public class TestClassC
{
    public int IntProperty { get; set; }

    public string DoubleProperty { get; set; }
}

public class TestClassD
{
    public float FloatProperty { get; set; }

    public double DoubleProperty { get; set; }

    public string StringProperty { get; set; }

    public TestClassC ValueC { get; set; }
}

public class TestClassE
{
    public Guid TestClassId { get; set; }

    public IEnumerable<TestClassD> Values { get; set; }
}

public class TestClassF
{
    public Guid TestClassId { get; set; }

    public TestClassD Value { get; set; }
}

public class TestClassG<T>
{
    public T InnerClass { get; set; }

    public Guid Id { get; set; }
}

public class Patient : ModelBase
{
    public long PatientId { get; set; }

    public string MRN { get; set; }

    public string LastName { get; set; }

    public string FirstName { get; set; }

    public string Gender { get; set; }

    public DateTime? DOB { get; set; }

    public bool CanAddScic { get; set; }

    public bool IsDirty { get; set; }
}

public abstract class ModelBase : INotifyPropertyChanged
{
#pragma warning disable CS0067
    public event PropertyChangedEventHandler PropertyChanged;
#pragma warning restore CS0067
}

[JsonSourceGenerationOptions(
    GenerationMode = JsonSourceGenerationMode.Metadata,
    IgnoreReadOnlyFields = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(TestClassA))]
[JsonSerializable(typeof(List<TestClassA>))]
[JsonSerializable(typeof(TestClassA))]
[JsonSerializable(typeof(TestClassB))]
[JsonSerializable(typeof(TestClassC))]
[JsonSerializable(typeof(TestClassD))]
[JsonSerializable(typeof(TestClassE))]
[JsonSerializable(typeof(TestClassF))]
[JsonSerializable(typeof(TestClassF))]
[JsonSerializable(typeof(Patient))]
[JsonSerializable(typeof(User))]
[JsonSerializable(typeof(List<User>))]
internal partial class TestJsonContext : JsonSerializerContext
{
}
