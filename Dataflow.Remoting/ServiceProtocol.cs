using System;
using System.Collections.Generic;
using System.Threading;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public class ServiceProtocol : AwaitIo
    {
        private SpinLock _lock;

        //--public IChannel Target { get; set; }
        //--public Request Request { get; set; }

        public int StillExecuting { get; protected set; }
        public bool KeepReading { get; protected set; }

        public ServiceProtocol()
            : base(false)
        {
            _lock = new SpinLock(false);
        }

        // prepares protocol handler instance to collect new request data.
        public virtual int BeginRequest()
        {
            return 0;
        }

        // analyze buffered content to set KeepReading to false when full request is accumulated.
        public virtual int CheckRequestData()
        {
            return 0;
        }

        // parses accumulated request(s) and start execution, returns number of still executing requests.
        public virtual int ParseExecuteRequest()
        {
            StillExecuting = 0;
            return 0;
        }

        //TODO : outdated cut-off implementation, may go virtual or improve...
        // waits for at least one outstanding request to finish and write data out, returns number of still executing requests.
        public AwaitIo WaitForExecuting()
        {
            int pending = 0;
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                pending = StillExecuting;
            }
            finally
            {
                if(lockTaken) _lock.Exit();
            }
            return pending > 0 ? this : AwaitIo.Done;
        }
    }
}
