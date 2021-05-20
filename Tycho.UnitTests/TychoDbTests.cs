using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using System.Threading;
using FluentAssertions.Common;
using System.Linq;

namespace Tycho.UnitTests
{
    [TestClass]
    public class TychoDbTests
    {
        [TestMethod]
        public async Task TychoDb_InsertObject_ShouldBeSuccessful ()
        {
            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);
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

        [TestMethod]
        public async Task TychoDb_InsertAndReadManyObjects_ShouldBeSuccessful ()
        {
            var expected = 1000;

            var successWrites = 0;
            var successReads = 0;

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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


                            var resultWrite = await db.WriteObjectAsync (testObj, x => x.StringProperty).ConfigureAwait(false);

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
                    .ToList();

            await Task.WhenAll (tasks).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            successWrites.Should ().Be (expected);
            successReads.Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_InsertManyObjects_ShouldBeSuccessful ()
        {
            var expected = true;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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

        [TestMethod]
        public async Task TychoDb_InsertManyObjectsWithNesting_ShouldBeSuccessful ()
        {
            var expected = true;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

            var rng = new Random ();

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
                        i =>
                        {

                            var testClassDs =
                                Enumerable
                                    .Range (100, 1000)
                                    .Select (
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
                                    .ToList ();


                            var testObj =
                                new TestClassE
                                {
                                    TestClassId = Guid.NewGuid(),
                                    Values = testClassDs,
                                };

                            return testObj;
                        })
                    .ToList ();

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var resultWrite = await db.WriteObjectsAsync (testObjs, x => x.TestClassId.ToString()).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            resultWrite.Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_ReadManyObjects_ShouldBeSuccessful ()
        {
            var expected = 1000;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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

            objs.Count().Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_ReadManyGenericObjects_ShouldBeSuccessful ()
        {
            var expected = 1000;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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


            await db.WriteObjectAsync (testObjs, x => x.GetHashCode().ToString()).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs = await db.ReadObjectsAsync<List<TestClassA>> ().ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_ReadManyInnerObjects_ShouldBeSuccessful ()
        {
            var expected = 1000;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

            var testObjs =
                Enumerable
                    .Range (100, 1000)
                    .Select (
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
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.TestClassId).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs = await db.ReadObjectsAsync<TestClassF, TestClassD> (x => x.Value).ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_ReadManyInnerObjectsWithLessThanFilter_ShouldBeSuccessful ()
        {
            var expected = 500;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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
                                        },
                                };

                            return testObj;
                        })
                    .ToList ();


            await db.WriteObjectsAsync (testObjs, x => x.TestClassId).ConfigureAwait (false);

            var stopWatch = System.Diagnostics.Stopwatch.StartNew ();

            var objs =
                await db
                    .ReadObjectsAsync<TestClassF, TestClassD> (
                        x => x.Value,
                        filter: new FilterBuilder<TestClassF> ()
                            .Filter (FilterType.LessThan, x => x.Value.DoubleProperty, 500));

            var count = objs.Count ();

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            count.Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_InsertObjectAndQuery_ShouldBeSuccessful ()
        {
            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);
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
            readResult.Should ().NotBeNull();
            readResult.StringProperty.Should ().Be (testObj.StringProperty);
            readResult.IntProperty.Should ().Be (testObj.IntProperty);
            readResult.TimestampMillis.Should ().Be (testObj.TimestampMillis);
        }

        [TestMethod]
        public async Task TychoDb_QueryUsingContains_ShouldBeSuccessful ()
        {
            var expected = 1000;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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
                        filter: new FilterBuilder<TestClassA>()
                            .Filter(FilterType.Contains, x => x.StringProperty, " String "))
                    .ConfigureAwait (false);

            stopWatch.Stop ();

            Console.WriteLine ($"Total Processing Time: {stopWatch.ElapsedMilliseconds}ms");

            objs.Count ().Should ().Be (expected);
        }

        [TestMethod]
        public async Task TychoDb_QueryInnerObjectUsingEquals_ShouldBeSuccessful ()
        {
            var expected = 1;

            var doubleProperty = 1234d;

            var db = new TychoDb (Path.GetTempPath (), rebuildCache: true);

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
}
