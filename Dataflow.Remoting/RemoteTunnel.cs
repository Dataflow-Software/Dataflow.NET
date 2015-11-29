/*
 * Remote tunnel implements client side of RPC channel, through which generated service proxy talks to remote.
 * © Viktor Poteryakhin, Dataflow Software Inc, 2014-2015. 
*/
using System;
using System.Threading;
using System.Threading.Tasks;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public partial class RemoteTunnel : IChannel
    {
        public delegate Connection ConnectionBuilder(Uri uri);
        public delegate ChannelProtocol ProtocolBuilder(Connection dtc);

        protected class ConnectionEntry
        {
            public ConnectionEntry _next, _prev;
            public Connection Connection { get; set; }
            public ChannelProtocol Protocol { get; set; }
            public int Status { get; set; }
            public int InitialPosition { get; set; }
        }

        private readonly ConnectionBuilder CreateConnection;
        private readonly ProtocolBuilder CreateProtocol;
        private ConnectionEntry _head, _tail;
        
        [Flags]
        private enum Option { None = 0, Connecting = 1, Connected = 2, RemoteDown = 4, Disconnecting = 8, Closed = 16, ConnectFailed = 32 }
        private Option _options;
        private SpinLock _lock;
        private int _lastIssued;

        private readonly RequestQueue _queue;
        private int _activeWorkers;
        private bool _openingConnection;

        public int ConnectionCount { get; protected set; }
        public int MaxConnections { get; protected set; }

        public bool Connected { get { return _options.HasFlag(Option.Connected); } }
        public bool Closed { get { return _options.HasFlag(Option.Closed); } }

        public Uri EndPoint { get; private set; }
        public int Pipeline { get; set; }
        public int Throttle { get; private set; }

        protected RemoteTunnel(int maxConnections)
        {
            if (maxConnections < 1 || maxConnections > 8192)
                throw new ArgumentException("maxConnections");
            MaxConnections = maxConnections;
            _queue = new RequestQueue();
        }

        public RemoteTunnel(string url, DataEncoding encoding = DataEncoding.Proto, int maxConnections = 2) : this(maxConnections)
        {
            if (url == null)
                throw new ArgumentNullException();
            EndPoint = new Uri(url);
            var throttle = 0;
            CreateConnection = ConnectionBuilderFromUri(EndPoint, out throttle);
            Throttle = throttle;
            switch (encoding)
            {
                case DataEncoding.Memcached:
                    CreateProtocol = (dtc) => { return new Memcached.ClientProtocol(dtc); }; break;
                default:
                    throw new NotImplementedException(encoding.ToString());
            }
        }

        public RemoteTunnel(ConnectionBuilder cbuilder, ProtocolBuilder pfactory, int maxConnections = 2, int throttle = 0)
            : this(maxConnections)
        {
            CreateConnection = cbuilder;
            CreateProtocol = pfactory;
            Throttle = throttle;
        }

        private static ConnectionBuilder ConnectionBuilderFromUri(Uri endPoint, out int throttle)
        {
            switch (endPoint.Scheme)
            {
                //case "http":
                //case "https":
                //    throttle = 150;
                //    return (ep) => { return new WebRequestConnection(ep); };
                //case "tcp":
                //    throttle = 50;
                //    return (ep) => { return new SocketConnection(ep); };
                //case "pipe":
                //    throttle = 35;
                //    return (ep) => { return new NamedPipeConnection(ep); };
                default: throw new NotSupportedException();
            }
            
        }

        public AwaitIo CloseAsync()
        {
            var lockTacken = false;
            try
            {
                _lock.Enter(ref lockTacken);
                _options |= Option.Closed;
            }
            finally
            {
                if (lockTacken) _lock.Exit();
            }
            return AwaitIo.Done; //todo: wait for all services to complete ??
        }

        private ConnectionEntry ConnectionAdded(ConnectionEntry entry)
        {
            var lockTacken = false;
            try
            {
                _lock.Enter(ref lockTacken);
                // double linked list add-to-tail method.
                if (_tail == null)
                    _tail = _head = entry;
                else
                {
                    _tail._next = entry;
                    entry._prev = _tail;
                    _tail = entry;
                }

                // update tracking stats on channel.
                entry.InitialPosition = _activeWorkers++;
                _lastIssued = Environment.TickCount;
            }
            finally
            {
                if (lockTacken) _lock.Exit();
            }

            return entry;
        }

        private bool ConnectionFailed(Exception ex, ConnectionEntry entry, bool wasOpening)
        {
            var lockTacken = false;
            var retry_connection = false;
            var fail_waiting = false;
            try
            {
                _lock.Enter(ref lockTacken);
                // double linked list remove method...
                if (entry == _tail)
                    if (entry == _head)
                        _head = _tail = null;
                    else
                    {
                        _tail = entry._prev;
                        _tail._next = null;
                    }
                else
                    if (entry == _head)
                    {
                        _head = entry._next;
                        _head._prev = null;
                    }
                    else
                    {
                        var prev = entry._prev;
                        var next = entry._next;
                        prev._next = next;
                        next._prev = prev;
                    }

                // schedule connection failure responses.
                if (wasOpening)
                {
                    // report all queued requests as failed when first activating connection attempt fail.
                    if (_activeWorkers == 0)
                        fail_waiting = true;
                    // extra new connections may simply fail due to remote resource provisioning, no action needed.
                    _openingConnection = false;
                }
                else
                {
                    // when all active connections fail and queue is not empty, will try to open new connection to revive link.
                    if (--_activeWorkers == 0 && _queue.QueuedCount > 0)
                        retry_connection = true;
                }
            }
            finally
            {
                if (lockTacken) _lock.Exit();
            }

            // we may need to retry opening link connection or fail or waiting requests.
            if (retry_connection)
                QueueConnectionService(true);
            if (fail_waiting)
                _queue.FailQueued();
            return false;
        }

        protected async Task AddConnectionService()
        {
            var resetOpenGate = true;
            var service = new ConnectionEntry { Connection = CreateConnection(EndPoint) };
            try
            {
                // trying to open new connection.
                await service.Connection.ConnectAsync();
                service.Protocol = CreateProtocol(service.Connection);
                ConnectionAdded(service);
                _openingConnection = resetOpenGate = false;

                // schedule async service on connection.
                await ConnectionService(service);
            }
            catch (Exception fault)
            {
                var dtc = service.Connection;
                ConnectionFailed(fault, service, resetOpenGate);
                var nowait = dtc.CloseAsync(true);
            }
        }

        protected async Task ConnectionService(ConnectionEntry service)
        {
            var dtc = service.Connection;
            var ptc = service.Protocol;
            var batch = new RequestQueue.Batch();
            do
            {
                // when queue was empty at the call moment, wait for batch to receive requests.
                if (!_queue.Dequeue(batch))
                    await batch;
                try
                {
                    // serializes requests in batch with proper formatting into connection DataStorage.
                    ptc.CreateRequest(batch.GetRequests());

                    // sends data content with proper connection level framing when needed.
                    await dtc.SendAsync();

                    // setup protocol state(and connection if needed) to process response stream.
                    ptc.BeginResponse();

                    // protocol ParseResponse will return true when no more data is needed from the connection.
                    do await dtc.ReadAsync();
                    while (!ptc.ParseResponse());
                }
                finally
                {
                    // will close resource handles and report/complete remaining requests.
                    ptc.EndResponse();
                    // some of the requests in the batch may be one way or "quiet" response model.
                    batch.CompleteRemaining();
                }
            }
            while (dtc.KeepAlive);

            // close the connection gracefully when service is stopped.
            await dtc.CloseAsync(false);
        }

        protected void QueueConnectionService(bool immediately = false)
        {
            // fast path to avoid costlier check when throttling.
            if (!immediately && Environment.TickCount - _lastIssued < Throttle)
                return;

            bool lockTacken = false, openConnection = false;
            try
            {
                _lock.Enter(ref lockTacken);
                if (!_openingConnection && (_activeWorkers == 0 || _activeWorkers < MaxConnections))
                {
                    _openingConnection = openConnection = true;
                }
            }
            finally
            {
                if (lockTacken) _lock.Exit();
            }

            if (!openConnection)
                return;
            Task.Run(AddConnectionService);
        }

        public void QueueRequest(Request request)
        {
            var waitingRequests = _queue.Enqueue(request);

            // waiting == 0 means new request was picked up for processing, > 1 means we tried new connection attempt.
            if (waitingRequests != 1 )
                return;
            QueueConnectionService();
        }

        #region IChannelAsync interface implementation 

        void IChannel.DispatchAsync(Request request)
        {
            QueueRequest(request);
        }

        #endregion
    }
}
