using System;
using System.Collections.Generic;

namespace Tycho.Benchmarks
{
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

        public double DoubleProperty { get; set; }
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
}
