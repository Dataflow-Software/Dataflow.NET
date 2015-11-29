using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public class AwaitIo : Awaitable<int> 
    {
        public readonly static AwaitIo Done = new AwaitIo(true);
        public AwaitIo(bool completed, int result = 0) : base(completed) { Result = result; }
    }

    public class Request2<T> : ICriticalNotifyCompletion where T : Message, new()
    {
        private readonly static Action Sentinel = () => { };
        private Action _continuation;

        public ServiceMethod Action { get; protected set; }
        public Message Params { get; set; }
        public T Result { get; set; }
        public Signal Fault { get; set; }
        public int Id { get; set; }

        public Request2<T> GetAwaiter()
        {
            return this;
        }

        public bool IsCompleted { get; private set; }

        public T GetResult()
        {
            if (Fault == null)
                return Result;
            throw Fault.GetException();
        }

        void INotifyCompletion.OnCompleted(Action continuation)
        {
            UnsafeOnCompletedEx(continuation);
        }

        void ICriticalNotifyCompletion.UnsafeOnCompleted(Action continuation)
        {
            UnsafeOnCompletedEx(continuation);
        }

        private void UnsafeOnCompletedEx(Action continuation)
        {
            if (_continuation == Sentinel || Interlocked.CompareExchange(ref _continuation, continuation, null) == Sentinel)
                continuation();
        }
    }

    public class Awaitable<T> : ICriticalNotifyCompletion
    {
        public readonly static Action Sentinel = () => { };

        private Action _continuation;
        private bool _completed;

        public Signal Fault { get; protected set; }
        public T Result { get; protected set; }

        public Awaitable()
        {
        }

        public Awaitable(bool completed)
        {
            _completed = completed;
        }

        public void CompleteAsync(T result)
        {
            Result = result;
            OnCompleteAsync();
        }

        public void CompleteAsync(Signal fault, T result = default(T))
        {
            Fault = fault;
            Result = result;
            OnCompleteAsync();
        }

        public void CompleteAsync(Exception fault)
        {
            Fault = new Signal.Runtime(fault);
            OnCompleteAsync();
        }

        public void CompleteAsync()
        {
            OnCompleteAsync();
        }

        protected void OnCompleteAsync()
        {
            //_completed = true;
            var continuation = _continuation ?? Interlocked.CompareExchange(ref _continuation, Sentinel, null);
            if (continuation != null)
                continuation();
        }

        public void CompleteSync(T result)
        {
            Result = result;
            _completed = true;
        }

        public void CompleteSync(Signal fault, T result = default(T))
        {
            Fault = fault;
            Result = result;
            _completed = true;
        }

        public void Reset()
        {
            _continuation = null;
            _completed = false;
            Result = default(T);
            Fault = null;
        }

        // C# await pattern implementation.

        public Awaitable<T> GetAwaiter()
        {
            return this;
        }

        public bool IsCompleted
        {
            get { return _completed; }
        }

        public T GetResult()
        {
            if (Fault == null) 
                return Result;
            throw Fault.GetException();
        }

        public void OnCompleted(Action continuation)
        {
            UnsafeOnCompleted(continuation);
        }

        public void UnsafeOnCompleted(Action continuation)
        {
            if (_continuation == Sentinel || Interlocked.CompareExchange(ref _continuation, continuation, null) == Sentinel)
            {
                Task.Run(continuation);
            }
        }

    }
}
