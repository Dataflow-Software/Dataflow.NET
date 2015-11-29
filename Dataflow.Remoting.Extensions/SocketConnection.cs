using System;
using System.Net;

namespace Dataflow.Remoting
{
    public class SocketConnection : Connection
    {
        internal SaeSocket _socket;
        public IPEndPoint Remote { get; private set; }
        public int Timeout { get; set; }

        public SocketConnection(Uri ep)
            : base(ep)
        {
            KeepAlive = true;
        }

        public SocketConnection(IPEndPoint ep, bool keep_alive)
            : base(null)
        {
            Remote = ep;
            KeepAlive = keep_alive;
        }

        public override AwaitIo CloseAsync(bool panic = false)
        {
            var socket = _socket;
            if (socket == null)
                return AwaitIo.Done;
            _socket = null;
            return socket.CloseAsync(panic);
        }

        public override AwaitIo ConnectAsync()
        {
            if (_socket != null && _socket.Status == SaeSocket.State.Connected) 
                return AwaitIo.Done;
            _socket = new SaeSocket(_awaitable, Data);
            if (Remote == null)
                Remote = SaeSocket.GetRemoteEp(EndPoint.DnsSafeHost, EndPoint.Port);
            return _socket.ConnectAsync(Remote, Timeout);
        }

        public override AwaitIo ReadAsync()
        {
            return _socket.ReceiveAsync();
        }
 
        public override AwaitIo SendAsync()
        {
            return _socket.SendAsync();
        }
    }
}
