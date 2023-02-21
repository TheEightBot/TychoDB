using System;
using System.Collections.Generic;
using SQLite;

namespace Tycho.Benchmarks
{
    public class TestClassA
    {
        [PrimaryKey]
        public string StringProperty { get; set; }

        public long LongProperty { get; set; }

        public long TimestampMillis { get; set; }
    }

    public class TestClassB
    {
        public string StringProperty { get; set; }

        public double DoubleProperty { get; set; }
    }

    public class TestClassC
    {
        [PrimaryKey]
        public int IntProperty { get; set; }

        public double DoubleProperty { get; set; }
    }

    public class TestClassD
    {
        [PrimaryKey]
        public float FloatProperty { get; set; }

        public double DoubleProperty { get; set; }

        public TestClassC ValueC { get; set; }
    }

    public class TestClassE
    {
        [PrimaryKey]
        public Guid TestClassId { get; set; }

        public IEnumerable<TestClassD> Values { get; set; }
    }

    public class TestClassF
    {
        public Guid TestClassId { get; set; }

        public TestClassD Value { get; set; }
    }
}
