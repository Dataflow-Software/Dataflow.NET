using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public interface IChannelSync
    {
        void Dispatch(Message request, Message response);
    }

    public interface IChannel
    {
        void DispatchAsync(Request request);
    }

    public class ChannelSync : IChannelSync, IRequestTracker
    {
        private IChannel _channel;
        private AutoResetEvent _event;
        private Request _current;
        private int _timeout;

        public ChannelSync(IChannel channel)
        {
            _channel = channel;
            _event = new AutoResetEvent(false);
            _timeout = 15000;
        }

        public void Dispatch(Message request, Message response)
        {
            _current = new Request(this, request, response, null);
            _channel.DispatchAsync(_current);
            //TODO: probably need some form of CompareExchange protection on _current/etc to keep reliable event state.
            if(_current.IsCompleted)
                return;
            if (!_event.WaitOne(_timeout))
            {
                _current = null;
                throw new TimeoutException();
            }
        }

        public void DispatchBatch(Message[] request, Message[] response)
        {
            throw new NotImplementedException();
        }

        public void Completed(Request request)
        {
            // can get here on misfire or time-expired call.
            if (request == _current)
                _event.Set();
        }
    }
}
