using System;
using System.Net.Sockets;
using Dataflow.Remoting;

namespace Dataflow.Remoting
{

    public class SocketListenerConnection : SocketConnection
    {
        private readonly SocketListener _listener;

        public SocketListenerConnection(SocketListener host)
            : base(null)
        {
            _listener = host;
        }

        public override AwaitIo AcceptAsync()
        {
            if (_socket != null) 
                throw new InvalidOperationException();
            KeepAlive = true;
            _socket = new SaeSocket(_awaitable, Data);
            return _socket.AcceptAsync(_listener.Socket, _listener.PostAcceptBuffers ? Data : null);
        }
    }

    public class SocketListener : Listener
    {
        public Socket Socket { get; protected set; }
        public int Backlog { get; protected set; }
        public bool PostAcceptBuffers { get; set; }

        public SocketListener(int backlog = 100) : base()
        {
            Backlog = backlog;
        }

        protected override Connection CreateConnection() 
        { 
            return new SocketListenerConnection(this); 
        }

        protected override void StartService()
        {
            var lcep = SaeSocket.GetRemoteEp(EndPoint.DnsSafeHost, EndPoint.Port);
            Socket = new Socket(lcep.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            Socket.Bind(lcep);
            Socket.Listen(Backlog);
        }

        protected override void StopService()
        {
            if (Socket == null) return;
            var lsnr = Socket;
            Socket = null;
            lsnr.Close();
        }
    }
}
