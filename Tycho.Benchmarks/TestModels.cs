using System;
using System.Collections.Generic;
using SQLite;

namespace Tycho.Benchmarks
{
    class TestClassA
    {
        [PrimaryKey]
        public string StringProperty { get; set; }

        public long LongProperty { get; set; }

        public long TimestampMillis { get; set; }
    }


    class TestClassB
    {
        public string StringProperty { get; set; }

        public double DoubleProperty { get; set; }
    }

    class TestClassC
    {
        [PrimaryKey]
        public int IntProperty { get; set; }

        public double DoubleProperty { get; set; }
    }

    class TestClassD
    {
        [PrimaryKey]
        public float FloatProperty { get; set; }

        public double DoubleProperty { get; set; }

        public TestClassC ValueC { get; set; }
    }

    class TestClassE
    {
        [PrimaryKey]
        public Guid TestClassId { get; set; }

        public IEnumerable<TestClassD> Values { get; set; }
    }

    class TestClassF
    {
        public Guid TestClassId { get; set; }

        public TestClassD Value { get; set; }
    }
}
