using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public interface IRequestTracker
    {
        void Completed(Request request);
    }

    public class Request
    {
        private IRequestTracker _tracker;

        public ServiceMethod Action { get; protected set; }
        public Message Context { get; private set; }
        public Message Params { get; set; }
        public Message Result { get; set; }
        public Signal Fault { get; set; }
        public int Id { get; set; }

        public bool IsCompleted { get { return _tracker == null; } }

        [DebuggerStepThrough]
        public Request(IRequestTracker tracker)
        {
            _tracker = tracker;
        }

        [DebuggerStepThrough]
        public Request(IRequestTracker tracker, Message args, Message rets, ServiceMethod action)
        {
            _tracker = tracker;
            Action = action;
            Params = args;
            Result = rets;
        }

        public void Complete()
        {
            var tracker = _tracker;
            if (tracker != null)
            {
                _tracker = null;
                tracker.Completed(this);
            }
        }

        public void Fail(Signal fault)
        {
            Fault = fault;
            Complete();
        }
    }


    public class AwaitableRequest<TR> : Awaitable<TR>, IRequestTracker
        where TR : Message, new()
    {
        public AwaitableRequest()
        {
            Result = new TR();
        }

        void IRequestTracker.Completed(Request request)
        {
            if (request.Fault != null)
                Fault = request.Fault;
            CompleteAsync();
        }
    }

    // other scenarios, like complete-on-first(done/fail) to be implemented later.
    public class AwaitableBatch<TR, TP> : Awaitable<TR[]>, IRequestTracker
        where TR : Message, new()
        where TP : Message
    {
        private Request[] _batch;
        private int _pending;

        private Repeated<TP> _params;
        public TP[] Params { get { return _params.Items; } }

        public AwaitableBatch()
        {
        }

        public TP AddRequest(TP request)
        {
            if (_batch == null)
                return _params.Add(request);
            throw new InvalidOperationException("batch was executed");
        }

        public Request[] GetRequests()
        {
            if (_params.Count == 0)
                throw new InvalidOperationException("batch is empty");
            var args = _params.Items;
            _pending = args.Length;
            var rets = new TR[_pending];
            for (int i = 0; i < rets.Length; i++)
                rets[i] = new TR();
            Result = rets;

            _batch = new Request[_pending];
            for (int i = 0; i < _batch.Length; i++)
                _batch[i] = new Request(this, args[i], rets[i], null);
            return _batch;
        }

        // simplest logic, waits for all requests to report.

        // todo: we must record failed requests exceptions too.

        void IRequestTracker.Completed(Request request)
        {
            if (Interlocked.Decrement(ref _pending) > 0)
                return;
            OnCompleteAsync();
        }
    }
}
