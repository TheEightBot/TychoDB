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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
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
                    [typeof(DateTimeTestRecord)] = TestJsonContext.Default.DateTimeTestRecord,
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

        result.ShouldBeTrue();
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

        result.ShouldBeTrue();
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

        result.ShouldBeTrue();
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

        result.ShouldBeTrue();
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

        result.ShouldBeTrue();
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

        result.ShouldNotBeEmpty();
        result.Count().ShouldBe(1);
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

                        if (resultRead is not null)
                        {
                            Interlocked.Increment(ref successReads);
                        }
                    })
                .ToList();

        await Task.WhenAll(tasks).ConfigureAwait(false);

        stopWatch.Stop();

        Console.WriteLine($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

        successWrites.ShouldBe(expected);
        successReads.ShouldBe(expected);
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

        resultWrite.ShouldBe(expected);
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

        resultWrite.ShouldBe(expected);
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

        resultWrite.ShouldBe(expected);
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

        resultReadCount.ShouldBe(expected);
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

        objs.Count().ShouldBe(testObjs.Count);
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

        objs.Count().ShouldBe(expected);
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

        objsCount.ShouldBe(expected);
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

        obj.Count().ShouldBe(expected);
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

        exists.ShouldBe(expected);
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

        objs.Count().ShouldBe(expected);
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

        objs.Count().ShouldBe(expected);
        objs.All(x => Guid.Parse(x.Key) == Guid.Parse(x.InnerObject.StringProperty)).ShouldBeTrue();
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

        objs.Count().ShouldBe(expected);
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

        count.ShouldBe(expected);
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

        count.ShouldBe(expected);
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

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.StringProperty.ShouldBe(testObj.StringProperty);
        readResult.IntProperty.ShouldBe(testObj.IntProperty);
        readResult.TimestampMillis.ShouldBe(testObj.TimestampMillis);
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

        objs.Count().ShouldBe(expected);
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

        obj.ShouldNotBeNull();
        obj.TestClassId.ShouldBe(expected);
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

        obj.ShouldNotBeNull();
        obj.TestClassId.ShouldBe(expected);
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

        objs.Count().ShouldBe(expected);
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

        objs.Count().ShouldBe(expected);
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

        writeSuccessful.ShouldBeTrue();

        var objs =
            await db
                .ReadObjectsAsync<Patient>(
                    filter: FilterBuilder<Patient>
                        .Create()
                        .Filter(FilterType.Equals, x => x.DOB, testObj.DOB))
                .ConfigureAwait(false);

        objs.Count().ShouldBe(expected);
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

        objs.First().PatientId.ShouldBe(expectedFirstId);
        objs.Last().PatientId.ShouldBe(expectedLastId);
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

        successful.ShouldBe(expected);
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

        successful.ShouldBe(expected);
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

        successful.ShouldBe(expected);
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

        writeResult.ShouldBeTrue();
        deleteResult.ShouldBeTrue();
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

        objsDeleted.ShouldBe(expectedCount);
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

        result.ShouldBeTrue();
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

        insertResult.ShouldBeTrue();
        streamContents.ShouldBeEquivalentTo(textExample);
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

        insertResult.ShouldBeTrue();
        deleteResult.ShouldBeTrue();
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

        insertResult.ShouldBeTrue();
        existsResult.ShouldBeTrue();
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

        deleteResult.Successful.ShouldBeTrue();
        deleteResult.Count.ShouldBe(expected);
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

        readA.IntProperty.ShouldBe(classAIntProperty);
        readB.DoubleProperty.ShouldBe(classBDoubleProperty);
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

        readA.IntProperty.ShouldBe(obj1IntProperty);
        readB.IntProperty.ShouldBe(obj2IntProperty);
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

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadDateTime_WithFixedKeyAndPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_partition";
        var key = "test_key1";
        var createdDate = new DateTime(2025, 10, 6, 12, 30, 45, DateTimeKind.Utc);

        var testObj =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = createdDate,
                ModifiedDate = null,
                EventTimestamp = DateTimeOffset.UtcNow,
                LastAccessTimestamp = null,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Id.ShouldBe(key);
        readResult.CreatedDate.ShouldBe(createdDate);
        readResult.ModifiedDate.ShouldBeNull();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadNullableDateTime_WithFixedKeyAndPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_partition";
        var key = "test_key2";
        var createdDate = new DateTime(2025, 10, 6, 12, 30, 45, DateTimeKind.Utc);
        var modifiedDate = new DateTime(2025, 10, 6, 14, 15, 30, DateTimeKind.Utc);

        var testObj =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = createdDate,
                ModifiedDate = modifiedDate,
                EventTimestamp = DateTimeOffset.UtcNow,
                LastAccessTimestamp = null,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Id.ShouldBe(key);
        readResult.CreatedDate.ShouldBe(createdDate);
        readResult.ModifiedDate.ShouldNotBeNull();
        readResult.ModifiedDate.Value.ShouldBe(modifiedDate);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadDateTimeOffset_WithFixedKeyAndPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_partition";
        var key = "test_key3";
        var eventTimestamp = new DateTimeOffset(2025, 10, 6, 12, 30, 45, TimeSpan.FromHours(-5));

        var testObj =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = DateTime.UtcNow,
                ModifiedDate = null,
                EventTimestamp = eventTimestamp,
                LastAccessTimestamp = null,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Id.ShouldBe(key);
        readResult.EventTimestamp.ShouldBe(eventTimestamp);
        readResult.LastAccessTimestamp.ShouldBeNull();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadNullableDateTimeOffset_WithFixedKeyAndPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_partition";
        var key = "test_key4";
        var eventTimestamp = new DateTimeOffset(2025, 10, 6, 12, 30, 45, TimeSpan.FromHours(-5));
        var lastAccessTimestamp = new DateTimeOffset(2025, 10, 6, 15, 45, 30, TimeSpan.FromHours(-5));

        var testObj =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = DateTime.UtcNow,
                ModifiedDate = null,
                EventTimestamp = eventTimestamp,
                LastAccessTimestamp = lastAccessTimestamp,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Id.ShouldBe(key);
        readResult.EventTimestamp.ShouldBe(eventTimestamp);
        readResult.LastAccessTimestamp.ShouldNotBeNull();
        readResult.LastAccessTimestamp.Value.ShouldBe(lastAccessTimestamp);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadAllDateTimeTypes_WithFixedKeyAndPartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_partition";
        var key = "test_key5";
        var createdDate = new DateTime(2025, 10, 6, 12, 30, 45, DateTimeKind.Utc);
        var modifiedDate = new DateTime(2025, 10, 6, 14, 15, 30, DateTimeKind.Utc);
        var eventTimestamp = new DateTimeOffset(2025, 10, 6, 12, 30, 45, TimeSpan.FromHours(-5));
        var lastAccessTimestamp = new DateTimeOffset(2025, 10, 6, 15, 45, 30, TimeSpan.FromHours(-5));

        var testObj =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = createdDate,
                ModifiedDate = modifiedDate,
                EventTimestamp = eventTimestamp,
                LastAccessTimestamp = lastAccessTimestamp,
            };

        var writeResult = await db.WriteObjectAsync(testObj, x => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Id.ShouldBe(key);
        readResult.CreatedDate.ShouldBe(createdDate);
        readResult.ModifiedDate.ShouldNotBeNull();
        readResult.ModifiedDate.Value.ShouldBe(modifiedDate);
        readResult.EventTimestamp.ShouldBe(eventTimestamp);
        readResult.LastAccessTimestamp.ShouldNotBeNull();
        readResult.LastAccessTimestamp.Value.ShouldBe(lastAccessTimestamp);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadMultipleDateTimeRecords_WithDifferentKeysInSamePartition_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_partition";
        var key1 = "test_key6";
        var key2 = "test_key7";
        var createdDate1 = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var createdDate2 = new DateTime(2025, 12, 31, 23, 59, 59, DateTimeKind.Utc);

        var testObj1 =
            new DateTimeTestRecord
            {
                Id = key1,
                CreatedDate = createdDate1,
                ModifiedDate = null,
                EventTimestamp = DateTimeOffset.UtcNow,
                LastAccessTimestamp = null,
            };

        var testObj2 =
            new DateTimeTestRecord
            {
                Id = key2,
                CreatedDate = createdDate2,
                ModifiedDate = createdDate2.AddHours(1),
                EventTimestamp = DateTimeOffset.UtcNow,
                LastAccessTimestamp = DateTimeOffset.UtcNow.AddHours(-2),
            };

        var writeResult1 = await db.WriteObjectAsync(testObj1, x => key1, partition: partition);
        var writeResult2 = await db.WriteObjectAsync(testObj2, x => key2, partition: partition);
        var readResult1 = await db.ReadObjectAsync<DateTimeTestRecord>(key1, partition);
        var readResult2 = await db.ReadObjectAsync<DateTimeTestRecord>(key2, partition);

        writeResult1.ShouldBeTrue();
        writeResult2.ShouldBeTrue();
        readResult1.ShouldNotBeNull();
        readResult2.ShouldNotBeNull();
        readResult1.Id.ShouldBe(key1);
        readResult1.CreatedDate.ShouldBe(createdDate1);
        readResult2.Id.ShouldBe(key2);
        readResult2.CreatedDate.ShouldBe(createdDate2);
        readResult2.ModifiedDate.ShouldNotBeNull();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadDateTimeRecord_WithSameKeyInDifferentPartitions_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition1 = "partition_1";
        var partition2 = "partition_2";
        var key = "test_key8";
        var createdDate1 = new DateTime(2025, 5, 1, 10, 0, 0, DateTimeKind.Utc);
        var createdDate2 = new DateTime(2025, 5, 15, 15, 30, 0, DateTimeKind.Utc);

        var testObj1 =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = createdDate1,
                ModifiedDate = null,
                EventTimestamp = new DateTimeOffset(2025, 5, 1, 10, 0, 0, TimeSpan.Zero),
                LastAccessTimestamp = null,
            };

        var testObj2 =
            new DateTimeTestRecord
            {
                Id = key,
                CreatedDate = createdDate2,
                ModifiedDate = createdDate2.AddDays(1),
                EventTimestamp = new DateTimeOffset(2025, 5, 15, 15, 30, 0, TimeSpan.Zero),
                LastAccessTimestamp = new DateTimeOffset(2025, 5, 16, 8, 0, 0, TimeSpan.Zero),
            };

        var writeResult1 = await db.WriteObjectAsync(testObj1, _ => key, partition: partition1);
        var writeResult2 = await db.WriteObjectAsync(testObj2, _ => key, partition: partition2);
        var readResult1 = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition1);
        var readResult2 = await db.ReadObjectAsync<DateTimeTestRecord>(key, partition2);

        writeResult1.ShouldBeTrue();
        writeResult2.ShouldBeTrue();
        readResult1.ShouldNotBeNull();
        readResult2.ShouldNotBeNull();
        readResult1.CreatedDate.ShouldBe(createdDate1);
        readResult1.ModifiedDate.ShouldBeNull();
        readResult2.CreatedDate.ShouldBe(createdDate2);
        readResult2.ModifiedDate.ShouldNotBeNull();
        readResult2.LastAccessTimestamp.ShouldNotBeNull();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadDateTime_AsStandaloneValue_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var key = "datetime_value_1";
        var testValue = new DateTime(2025, 10, 6, 9, 30, 15, DateTimeKind.Utc);

        var writeResult = await db.WriteObjectAsync(testValue, _ => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTime>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldBe(testValue);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadNullableDateTime_AsStandaloneValue_WithValue_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var key = "nullable_datetime_value_1";
        DateTime? testValue = new DateTime(2025, 10, 6, 14, 45, 30, DateTimeKind.Utc);

        var writeResult = await db.WriteObjectAsync(testValue, _ => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTime?>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Value.ShouldBe(testValue.Value);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadNullableDateTime_AsStandaloneValue_WithNull_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var key = "nullable_datetime_null_1";
        DateTime? testValue = null;

        var writeResult = await db.WriteObjectAsync(testValue, _ => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTime?>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldBeNull();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadDateTimeOffset_AsStandaloneValue_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var key = "datetimeoffset_value_1";
        var testValue = new DateTimeOffset(2025, 10, 6, 9, 30, 15, TimeSpan.FromHours(-5));

        var writeResult = await db.WriteObjectAsync(testValue, _ => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeOffset>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldBe(testValue);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadNullableDateTimeOffset_AsStandaloneValue_WithValue_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var key = "nullable_datetimeoffset_value_1";
        DateTimeOffset? testValue = new DateTimeOffset(2025, 10, 6, 14, 45, 30, TimeSpan.FromHours(2));

        var writeResult = await db.WriteObjectAsync(testValue, _ => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeOffset?>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldNotBeNull();
        readResult.Value.ShouldBe(testValue.Value);
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadNullableDateTimeOffset_AsStandaloneValue_WithNull_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var key = "nullable_datetimeoffset_null_1";
        DateTimeOffset? testValue = null;

        var writeResult = await db.WriteObjectAsync(testValue, _ => key, partition: partition);
        var readResult = await db.ReadObjectAsync<DateTimeOffset?>(key, partition);

        writeResult.ShouldBeTrue();
        readResult.ShouldBeNull();
    }

    [DataTestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task TychoDb_WriteAndReadMultipleStandaloneDateTimeValues_WithDifferentKeys_ShouldBeSuccessful(IJsonSerializer jsonSerializer)
    {
        using var db =
            BuildDatabaseConnection(jsonSerializer)
                .Connect();

        var partition = "datetime_standalone";
        var dateTimeKey = "multi_datetime";
        var dateTimeOffsetKey = "multi_datetimeoffset";
        var nullableDateTimeKey = "multi_nullable_datetime";
        var nullableDateTimeOffsetKey = "multi_nullable_datetimeoffset";

        var dateTimeValue = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc);
        var dateTimeOffsetValue = new DateTimeOffset(2025, 6, 20, 15, 30, 0, TimeSpan.FromHours(-7));
        DateTime? nullableDateTimeValue = new DateTime(2025, 8, 10, 8, 0, 0, DateTimeKind.Utc);
        DateTimeOffset? nullableDateTimeOffsetValue = new DateTimeOffset(2025, 12, 25, 20, 0, 0, TimeSpan.FromHours(1));

        await db.WriteObjectAsync(dateTimeValue, _ => dateTimeKey, partition: partition);
        await db.WriteObjectAsync(dateTimeOffsetValue, _ => dateTimeOffsetKey, partition: partition);
        await db.WriteObjectAsync(nullableDateTimeValue, _ => nullableDateTimeKey, partition: partition);
        await db.WriteObjectAsync(nullableDateTimeOffsetValue, _ => nullableDateTimeOffsetKey, partition: partition);

        var readDateTime = await db.ReadObjectAsync<DateTime>(dateTimeKey, partition);
        var readDateTimeOffset = await db.ReadObjectAsync<DateTimeOffset>(dateTimeOffsetKey, partition);
        var readNullableDateTime = await db.ReadObjectAsync<DateTime?>(nullableDateTimeKey, partition);
        var readNullableDateTimeOffset = await db.ReadObjectAsync<DateTimeOffset?>(nullableDateTimeOffsetKey, partition);

        readDateTime.ShouldBe(dateTimeValue);
        readDateTimeOffset.ShouldBe(dateTimeOffsetValue);
        readNullableDateTime.ShouldNotBeNull();
        readNullableDateTime.Value.ShouldBe(nullableDateTimeValue.Value);
        readNullableDateTimeOffset.ShouldNotBeNull();
        readNullableDateTimeOffset.Value.ShouldBe(nullableDateTimeOffsetValue.Value);
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

public class DateTimeTestRecord
{
    public string Id { get; set; }

    public DateTime CreatedDate { get; set; }

    public DateTime? ModifiedDate { get; set; }

    public DateTimeOffset EventTimestamp { get; set; }

    public DateTimeOffset? LastAccessTimestamp { get; set; }
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
[JsonSerializable(typeof(DateTimeTestRecord))]
internal partial class TestJsonContext : JsonSerializerContext
{
}
