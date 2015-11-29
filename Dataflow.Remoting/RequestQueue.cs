using System;
using System.Collections.Generic;
using System.Threading;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public class RequestQueue 
    {
        private static readonly Request[] EmptyBatch = new Request[0];

        private SpinLock _lock;
        private Batch _waitList;
        private Request[] _queue;
        private int _queuedCount;
        private int _head, _tail;

        public int MaxQueueSize { get; protected set; }
        public int QueuedCount { get { return _queuedCount; } }

        public class Batch : Awaitable<Request>
        {
            internal Batch _waitList;
            private Repeated<Request> _batch;
            
            public int BatchSize { get; set; }

            public int ExpiresOn { get; protected set; }

            public virtual bool AddRequest(Request request)
            {
                // default non-batching service
                if (BatchSize == 0)
                {
                    Result = request;
                    return false;
                }
                // add requests till batch is full; add more intelligent batch cut-off based on request payload size later 
                // there are issues like can't run payload size calcs under lock
                _batch.Add(request);
                return _batch.Count < BatchSize;
            }

            public Request[] GetRequests()
            {
                Request[] result = null;
                if(BatchSize == 0)
                {
                    result = new Request[1] { Result };
                }
                else
                {
                    result = _batch.Items;
                    //_batch.Clear();
                }
                return result;
            }

            public void CompleteRemaining()
            {
                if (BatchSize == 0)
                {
                    if (!Result.IsCompleted)
                        Result.Complete();
                    Result = null;
                }
                else
                {
                    foreach(var request in _batch.Items)
                        if (!request.IsCompleted)
                            request.Complete();
                    _batch.Clear();
                }

            }
        }

        public RequestQueue(int maxSize = 32000)
        {
            MaxQueueSize = maxSize;
            _queue = EmptyBatch;
            _lock = new SpinLock();
        }

        //private bool ClockEvent(Clock clock, uint elapsed)
        //{
            //if( !Connected ) return true;
            //todo: check if waiting fibers could be released.
           // return false;
        //}

        public void FailQueued()
        {
            //TODO: fail all queued events (for link down or other reasons).
        }

        public bool Dequeue(Batch batch)
        {
            bool success = false, lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                if (_queuedCount != 0)
                {
                    do
                    {
                        var request = _queue[_head];
                        var keepDequeing = batch.AddRequest(request);
                        if (++_head == _queue.Length)
                            _head = 0;
                        _queuedCount--;
                        if (!keepDequeing)
                            break;
                    }
                    while (_queuedCount > 0);
                    success = true;
                }
                else
                {
                    if (batch._waitList != null)
                        throw new InvalidOperationException("corrupt state");
                    batch._waitList = _waitList;
                    _waitList = batch;
                }
            }
            finally
            {
                if(lockTaken) _lock.Exit();
            }
            return success;
        }

        protected void EnqueueRequest(Request request)
        {
            var pos = _tail == _queue.Length ? 0 : _tail;
            _queue[pos] = request;
            _tail = pos + 1;
            _queuedCount++;
        }

        public int Enqueue(Request request)
        {
            Batch batch = null;
            var lockTacken = false;
            var result = 0;
            try
            {
                _lock.Enter(ref lockTacken);
                batch = _waitList;
                if (batch != null)
                {
                    _waitList = batch._waitList;
                    batch._waitList = null;
                    batch.AddRequest(request);
                }
                else
                {
                    if (_queuedCount < _queue.Length)
                    {
                        EnqueueRequest(request);
                    }
                    else
                        if (_queue.Length < MaxQueueSize)
                        {
                            var expandedSize = _queue.Length == 0 ? 8 : _queue.Length * 2;
                            if (expandedSize > MaxQueueSize) expandedSize = MaxQueueSize;
                            Array.Resize<Request>(ref _queue, expandedSize);
                            EnqueueRequest(request);
                        }
                        else
                            throw new InvalidOperationException("queue full");
                    result = _queuedCount;
                }
            }
            finally
            {
                if(lockTacken) _lock.Exit();
            }

            // if waiting batch was completed, schedule it.
            if (batch != null)
                batch.CompleteAsync();
            return result;
        }

    }
}
