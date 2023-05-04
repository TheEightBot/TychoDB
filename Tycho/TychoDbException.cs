using System;
using System.Runtime.Serialization;

namespace Tycho;

public class TychoDbException : Exception
{
    public TychoDbException()
    {
    }

    public TychoDbException(string message)
        : base(message)
    {
    }

    public TychoDbException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    protected TychoDbException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
}