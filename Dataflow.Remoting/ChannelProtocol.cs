using System;

namespace Dataflow.Remoting
{
    public class ProtocolException : Exception
    {
        public ProtocolException(string message) : base(message) { }
    }

    public abstract class ChannelProtocol
    {
        public Connection Connection { get; private set; }
        public Request[] Batch { get; protected set; }

        public ChannelProtocol(Connection dtc)
        {
            Connection = dtc;
        }

        public virtual void CreateRequest(Request[] batch) 
        { 
        }
        
        public virtual void BeginResponse() 
        { 
        }
        
        public virtual void EndResponse() 
        {
            Connection.Data.Reset();
        }

        public virtual bool ParseResponse() 
        { 
            return true; 
        }
    }
}
