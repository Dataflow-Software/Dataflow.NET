/*
 * Socket with await pattern support, using fastest available async I/O on .NET at the moment.
 * © Viktor Poteryakhin, Dataflow Software Inc, 2012-2015
*/
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public class SaeSocket : SocketAsyncEventArgs
    {
        private AwaitIo _awaitable;
        private DataStorage _data;
        private Socket _socket;

        public enum State { None = 0, Connected = 1, Closed = 2 };
        public State Status { get; private set; }

        public SaeSocket(AwaitIo awaitable, DataStorage dts) 
        {
            if (dts == null || awaitable == null)
                throw new ArgumentNullException();
            _awaitable = awaitable;
            _data = dts;
        }

        public SaeSocket(string host, int port)
        {
            RemoteEndPoint = GetRemoteEp(host, port);
        }

        public static IPEndPoint GetRemoteEp(string host, int port)
        {
            IPEndPoint ep;
            IPAddress addr;
            // n.n.n.n ip addresses must avoid GetHostEntry path.
            if (IPAddress.TryParse(host, out addr))
                ep = new IPEndPoint(addr, port);
            else
            {
                var he = Dns.GetHostEntry(host);
                // note: research on best selection from list, including round-robin etc.
                ep = new IPEndPoint(he.AddressList[he.AddressList.Length - 1], port);
            }
            return ep;
        }

        public AwaitIo AcceptAsync(Socket listener, bool post_read_buffers = false)
        {
            if (post_read_buffers)
            {
                var ds = _data.GetSegmentToRead();
                int pos = ds.Count, bts = ds.Size - pos;
                SetBuffer(ds.Buffer, ds.Offset + pos, bts);
            }
            // see Recv() comments on SuppressFlow.
            var async = false;
            var aFC = ExecutionContext.SuppressFlow();
            try { async = listener.AcceptAsync(this); } finally { aFC.Undo(); }
            return CheckCompleted(async);
        }

        public AwaitIo CloseAsync(bool panic = false)
        {
            if (Status == State.Closed)
                return AwaitIo.Done;

            // lock the closed state and dispose if socket was not open or panic-closed.
            Status = State.Closed;
            if (panic)
            {
                var sclose = _socket;
                _socket = null;
                sclose.Disconnect(false);
            }
            if (_socket == null)
            {
                Dispose();
                return AwaitIo.Done;
            }

            // async close is preferred under normal circumstances.
            DisconnectReuseSocket = true;
            var async = false;
            var aFC = ExecutionContext.SuppressFlow();
            try { async = _socket.DisconnectAsync(this); } finally { aFC.Undo(); }
            return CheckCompleted(async);
        }

        //- opens connections to host specified by remote property.
        public AwaitIo ConnectAsync(IPEndPoint ep, int timeout)
        {
            // validate current state.
            if (_socket != null) 
                throw new InvalidOperationException("socket");
            if (ep != null) RemoteEndPoint = ep;
            if (RemoteEndPoint == null) 
                throw new ArgumentException("remote");
            
            // prepare to send first chunk when data is there.
            if (_data.ContentSize > 0)
            {
                var ds = _data.GetSegmentToSend();
                SetBuffer(ds.Buffer, ds.Offset, ds.Count);
            }
            
            // create, configure and connect socket.
            _socket = new Socket(RemoteEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, UseOnlyOverlappedIO = true };
            if (timeout > 0)
                _socket.SendTimeout = _socket.ReceiveTimeout = timeout;

            // see RecvAsync() comments on SuppressFlow.
            var async = false;
            var aFC = ExecutionContext.SuppressFlow();
            try { async = _socket.ConnectAsync(this); } finally { aFC.Undo(); }
            return CheckCompleted(async);
        }

        public AwaitIo ReceiveAsync()
        {
            var ds = _data.GetSegmentToRead();
            var pos = ds.Count;
            SetBuffer(ds.Buffer, ds.Offset + pos, ds.Size - pos);
            // removing expensive .NET context flow support that is not used by us anyway. 
            var async = false;
            var aFC = ExecutionContext.SuppressFlow();
            try { async = _socket.ReceiveAsync(this); } finally { aFC.Undo(); }
            return CheckCompleted(async);
        }

        public AwaitIo SendAsync()
        {
            var ds = _data.GetSegmentToSend();
            SetBuffer(ds.Buffer, ds.Offset, ds.Count);
            // see RecvAsync() comments on SuppressFlow.
            var async = false;
            var aFC = ExecutionContext.SuppressFlow();
            try { async = _socket.SendAsync(this); } finally { aFC.Undo(); }
            return CheckCompleted(async);
        }


        private Signal ErrorToFault(SocketError se)
        {
            CloseAsync(true);
            return new Signal((int)se);
        }

        protected AwaitIo CheckCompleted(bool async)
        {
            if (async)
            {
                _awaitable.Reset();
                return _awaitable;
            }
            return OnCompleted(false);
        }

        protected override void OnCompleted(SocketAsyncEventArgs eas)
        {
            OnCompleted(true);
        }

        protected AwaitIo OnCompleted(bool async)
        {
            try
            {
                Signal fault = null;
                int result = BytesTransferred;
                if (SocketError == SocketError.Success)
                    switch (LastOperation)
                    {
                        case SocketAsyncOperation.Accept:
                            _socket = AcceptSocket;
                            _socket.UseOnlyOverlappedIO = true;
                            _socket.NoDelay = true;
                            if (BytesTransferred > 0)
                                _data.ReadCommit(BytesTransferred);
                            break;
                        case SocketAsyncOperation.Connect:
                            Status = State.Connected;
                            if (_data.ContentSize > 0)
                                goto case SocketAsyncOperation.Send;
                            break;
                        case SocketAsyncOperation.Receive:
                            if (BytesTransferred > 0)
                                _data.ReadCommit(BytesTransferred);
                            else 
                                fault = ErrorToFault(SocketError.ConnectionAborted);
                            break;
                        case SocketAsyncOperation.Send:
                            // send keeps iterating till last chunk is transmitted.
                            if (!_data.CommitSentBytes())
                                return SendAsync();
                            break;
                        case SocketAsyncOperation.Disconnect:
                            _socket = null;
                            Dispose();
                            break;
                        default: 
                            fault = ErrorToFault(SocketError.SocketError); 
                            break;
                    }
                else 
                    fault = ErrorToFault(SocketError);
                // async complete has bit higher overhead. 
                if (async)
                    _awaitable.CompleteAsync(fault, result);
                else _awaitable.CompleteSync(fault, result);
            }
            catch(Exception ex)
            {
                CloseAsync(true);
                _awaitable.CompleteAsync(ex);
            }
            return _awaitable;
        }
    }
}
