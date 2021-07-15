using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using System.Threading;
using FluentAssertions.Common;
using System.Linq;
using System.ComponentModel;

namespace Tycho.UnitTests
{
    [TestClass]
    public class TychoDbTests
    {
        private static readonly IJsonSerializer _systemTextJsonSerializer = new SystemTextJsonSerializer ();
        private static readonly IJsonSerializer _newtonsoftJsonSerializer = new NewtonsoftJsonSerializer ();

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
        public async Task TychoDb_InsertObject_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect();

            var testObj =
                new TestClassA
                {
                    StringProperty = "Test String",
                    IntProperty = 1984,
                    TimestampMillis = 123451234,
                };

            var result = await db.WriteObjectAsync (testObj, x => x.StringProperty);

            result.Should ().BeTrue ();
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_RegisterAndInsertObject_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .AddTypeRegistration<TestClassA> (x => x.StringProperty)
                    .Connect ();

            var testObj =
                new TestClassA
                {
                    StringProperty = "Test String",
                    IntProperty = 1984,
                    TimestampMillis = 123451234,
                };

            var result = await db.WriteObjectAsync (testObj);

            result.Should ().BeTrue ();
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_RegisterPatientAndInsertObject_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .AddTypeRegistration<Patient> (x => x.PatientId)
                    .Connect ();

            var testObj =
                new Patient
                {
                    PatientId = 12345,
                    FirstName = "TEST",
                    LastName = "PATIENT",
                };

            var result = await db.WriteObjectAsync (testObj);

            result.Should ().BeTrue ();
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_InsertAndReadManyObjects_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 1000;

            var successWrites = 0;
            var successReads = 0;

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var tasks =
                Enumerable
                    .Range (100, 1000)
                    .Select (
                        async i =>
                        {
                            var testObj =
                                new TestClassA
                                {
                                    StringProperty = $"Test String {i}",
                                    IntProperty = i,
                                    TimestampMillis = 123451234,
                                };


                            var resultWrite = await db.WriteObjectAsync (testObj, x => x.StringProperty).ConfigureAwait (false);

                            if (resultWrite)
                            {
                                Interlocked.Increment (ref successWrites);
                            }

                            var resultRead = await db.ReadObjectAsync<TestClassA> (testObj.StringProperty).ConfigureAwait (false);

                            if (resultRead != null)
                            {
                                Interlocked.Increment (ref successReads);
                            }
                        })
                    .ToList ();

            await Task.WhenAll (tasks).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            successWrites.Should ().Be (expected);
            successReads.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_InsertManyObjects_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = true;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
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
                    .ToList ();

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var resultWrite = await db.WriteObjectsAsync (testObjs, x => x.StringProperty).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            resultWrite.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_InsertManyObjectsWithNesting_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = true;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var rng = new Random ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
                        i =>
                        {

                            var testClassDs =
                                Enumerable
                                    .Range (10, 100)
                                    .Select (
                                        i =>
                                        {
                                            var testObj =
                                                new TestClassD
                                                {
                                                    DoubleProperty = rng.NextDouble (),
                                                    FloatProperty = (float)rng.NextDouble (),
                                                };

                                            return testObj;
                                        })
                                    .ToList ();


                            var testObj =
                                new TestClassE
                                {
                                    TestClassId = Guid.NewGuid (),
                                    Values = testClassDs,
                                };

                            return testObj;
                        })
                    .ToList ();

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var resultWrite = await db.WriteObjectsAsync (testObjs, x => x.TestClassId.ToString ()).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            resultWrite.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_InsertManyObjectsWithNestingAndFilterUsingGreaterThan_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 500;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var rng = new Random ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
                        i =>
                        {

                            var testClassDs =
                                Enumerable
                                    .Range (10, 10)
                                    .Select (
                                        ii =>
                                        {
                                            var testObj =
                                                new TestClassD
                                                {
                                                    DoubleProperty = rng.NextDouble (),
                                                    FloatProperty = i % 2 == 0 ? 251 : 0,
                                                };

                                            return testObj;
                                        })
                                    .ToList ();


                            var testObj =
                                new TestClassE
                                {
                                    TestClassId = Guid.NewGuid (),
                                    Values = testClassDs,
                                };

                            return testObj;
                        })
                    .ToList ();

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var resultWrite = await db.WriteObjectsAsync (testObjs, x => x.TestClassId.ToString ()).ConfigureAwait (false);

            var resultRead =
                await db
                    .ReadObjectsAsync<TestClassE> (
                        filter: new FilterBuilder<TestClassE> ()
                            .Filter (FilterType.GreaterThan, x => x.Values, x => x.FloatProperty, 250d));

            var resultReadCount = resultRead.Count ();

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            resultReadCount.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_ReadManyObjects_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 1000;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
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
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.StringProperty).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs = await db.ReadObjectsAsync<TestClassA> ().ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_ReadManyGenericObjects_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 1000;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
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
                    .ToList ();

            await db.WriteObjectAsync (testObjs, x => x.GetHashCode ()).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var obj = await db.ReadObjectAsync<List<TestClassA>> (testObjs.GetHashCode ()).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            obj.Count ().Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_ReadManyInnerObjects_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 1000;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
                        i =>
                        {
                            var testObj =
                                new TestClassF
                                {
                                    TestClassId = Guid.NewGuid (),
                                    Value =
                                        new TestClassD
                                        {
                                            DoubleProperty = 1234d,
                                            FloatProperty = 4567f,
                                        },
                                };

                            return testObj;
                        })
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.TestClassId).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs = await db.ReadObjectsAsync<TestClassF, TestClassD> (x => x.Value).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_ReadManyInnerObjectsWithLessThanFilter_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 500;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (0, 1000)
                    .Select (
                        i =>
                        {
                            var testObj =
                                new TestClassF
                                {
                                    TestClassId = Guid.NewGuid (),
                                    Value =
                                        new TestClassD
                                        {
                                            DoubleProperty = i,
                                            FloatProperty = 4567f,
                                            ValueC =
                                                new TestClassC
                                                {
                                                    IntProperty = i * 2,
                                                }
                                        },
                                };

                            return testObj;
                        })
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.TestClassId).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs =
                await db
                    .ReadObjectsAsync<TestClassF, TestClassC> (
                        x => x.Value.ValueC,
                        filter: new FilterBuilder<TestClassF> ()
                            .Filter (FilterType.GreaterThan, x => x.Value.DoubleProperty, 250d)
                            .And ()
                            .Filter (FilterType.LessThanOrEqualTo, x => x.Value.DoubleProperty, 750d));

            var count = objs.Count ();

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            count.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_ReadManyInnerObjectsWithLessThanFilterAndIndex_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 750;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ()
                    .CreateIndex<TestClassF> (x => x.Value.ValueC.IntProperty, "ValueCInt");

            var testObjs =
                Enumerable
                    .Range (0, 1000)
                    .Select (
                        i =>
                        {
                            var testObj =
                                new TestClassF
                                {
                                    TestClassId = Guid.NewGuid (),
                                    Value =
                                        new TestClassD
                                        {
                                            DoubleProperty = i,
                                            FloatProperty = 4567f,
                                            ValueC =
                                                new TestClassC
                                                {
                                                    IntProperty = i,
                                                }
                                        },
                                };

                            return testObj;
                        })
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.TestClassId).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs =
                await db
                    .ReadObjectsAsync<TestClassF, TestClassC> (
                        x => x.Value.ValueC,
                        filter: new FilterBuilder<TestClassF> ()
                            .Filter (FilterType.GreaterThanOrEqualTo, x => x.Value.ValueC.IntProperty, 250d));

            var count = objs.Count ();

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            count.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_InsertObjectAndQuery_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObj =
                new TestClassA
                {
                    StringProperty = "Test String",
                    IntProperty = 1984,
                    TimestampMillis = 123451234,
                };

            var writeResult = await db.WriteObjectAsync (testObj, x => x.StringProperty);
            var readResult = await db.ReadObjectAsync<TestClassA> (testObj.StringProperty);

            writeResult.Should ().BeTrue ();
            readResult.Should ().NotBeNull ();
            readResult.StringProperty.Should ().Be (testObj.StringProperty);
            readResult.IntProperty.Should ().Be (testObj.IntProperty);
            readResult.TimestampMillis.Should ().Be (testObj.TimestampMillis);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_QueryUsingContains_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 1000;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
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
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.GetHashCode ().ToString ()).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs =
                await db
                    .ReadObjectsAsync<TestClassA> (
                        filter: new FilterBuilder<TestClassA> ()
                            .Filter (FilterType.Contains, x => x.StringProperty, " String "))
                    .ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_QueryInnerObjectUsingEquals_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = 1;

            var doubleProperty = 1234.0d;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObj =
                new TestClassF
                {
                    TestClassId = Guid.NewGuid (),
                    Value =
                        new TestClassD
                        {
                            DoubleProperty = doubleProperty,
                            FloatProperty = 5678f,
                        },
                };

            await db.WriteObjectAsync (testObj, x => x.TestClassId).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs =
                await db
                    .ReadObjectsAsync<TestClassF> (
                        filter: new FilterBuilder<TestClassF> ()
                            .Filter (FilterType.Equals, x => x.Value.DoubleProperty, doubleProperty))
                    .ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_CreateDataIndex_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expected = true;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var successful = await db.CreateIndexAsync<TestClassD> (x => x.DoubleProperty, "double_index");

            successful.Should ().Be (expected);
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_DeleteObject_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObj =
                new TestClassA
                {
                    StringProperty = "Test String",
                    IntProperty = 1984,
                    TimestampMillis = 123451234,
                };

            var writeResult = await db.WriteObjectAsync (testObj, x => x.StringProperty);
            var deleteResult = await db.DeleteObjectAsync<TestClassA> (testObj.StringProperty);

            writeResult.Should ().BeTrue ();
            deleteResult.Should ().BeTrue ();
        }

        [DataTestMethod]
        [DynamicData (nameof (JsonSerializers))]
        public async Task TychoDb_DeleteManyObjects_ShouldBeSuccessful (IJsonSerializer jsonSerializer)
        {
            var expectedSuccess = true;
            var expectedCount = 1000;

            using var db =
                new TychoDb (Path.GetTempPath (), jsonSerializer, rebuildCache: true)
                    .Connect ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
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
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.StringProperty).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs = await db.DeleteObjectsAsync<TestClassA> ().ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Successful.Should ().Be (expectedSuccess);
            objs.Count.Should ().Be (expectedCount);
        }
    }

    class TestClassA
    {
        public string StringProperty { get; set; }

        public int IntProperty { get; set; }

        public long TimestampMillis { get; set; }
    }


    class TestClassB
    {
        public string StringProperty { get; set; }

        public double DoubleProperty { get; set; }
    }

    class TestClassC
    {
        public int IntProperty { get; set; }

        public string DoubleProperty { get; set; }
    }

    class TestClassD
    {
        public float FloatProperty { get; set; }

        public double DoubleProperty { get; set; }

        public TestClassC ValueC { get; set; }
    }

    class TestClassE
    {
        public Guid TestClassId { get; set; }

        public IEnumerable<TestClassD> Values { get; set; }
    }

    class TestClassF
    {
        public Guid TestClassId { get; set; }

        public TestClassD Value { get; set; }
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
        public event PropertyChangedEventHandler PropertyChanged;
    }
}
