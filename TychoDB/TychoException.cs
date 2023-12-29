using System;
using System.Runtime.Serialization;

namespace TychoDB;

public class TychoException : Exception
{
    public TychoException()
    {
    }

    public TychoException(string message)
        : base(message)
    {
    }

    public TychoException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
