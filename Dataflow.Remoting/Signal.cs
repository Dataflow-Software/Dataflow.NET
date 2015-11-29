using System;
using System.Collections.Generic;

namespace Dataflow.Remoting
{
    public class Signal
    {
        public const int
            iBadIp = -1, iBadRpc = -2, iBadState = -3, iArgument = -4, iNotImplemented = -5, iBadLpc = -6,
            iTimeout = 3, iRemoteDown = 4, iStopWorker = 5, iQueueFull = 6;

        public int ErrorCode { get; protected set; }
        public string Message  { get; protected set; }

        public Signal() { }
        public Signal(int ec) { ErrorCode = ec; }
        public Signal(string msg) { Message = msg; }
        public Signal(int ec, string msg) { ErrorCode = ec; Message = msg; }
        //public Signal( RpcError error ) { Message = error.Message; Code = error.Code; }

        public virtual System.Exception GetException()
        {
            return new Signal.Exception(this);
        }

        public class Runtime : Signal
        {
            public System.Exception fault;
            public Runtime(System.Exception ex)
            {
                Message = ex.Message;
                fault = ex;
            }

            public override System.Exception GetException()
            {
                return fault;
            }
        }

        public class Exception : System.Exception
        {
            public Signal Signal;
            public Exception(Signal signal) : base(signal.Message) { Signal = signal; }
        }
    }

    //public sealed partial class RpcFault
    //{
    //    const string SystemError = "system", sHttpError = "http", sRpcError = "rpc";
    //    public static RpcFault
    //        BadIp = new RpcFault(1, SystemError, null),
    //        NotImplemented = new RpcFault(2, SystemError, "not implemented"),
    //        Timeout = new RpcFault(3, SystemError, "timeout"),
    //        NotFound = new RpcFault(404, sHttpError, null);
    //    public RpcFault() { }
    //    public RpcFault(int ec, string type, string msg) { Status = ec; Name = type; Message = msg; }
    //    public RpcFault(Signal sig) { Status = sig.ErrorCode; Name = sig.GetType().Name; Message = sig.Message; }
    //}
}
