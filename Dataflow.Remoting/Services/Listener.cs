using System;
using Dataflow.Serialization;
using System.Threading.Tasks;

namespace Dataflow.Remoting
{
    public partial class Listener : IDisposable
    {
        public delegate ServiceProtocol ProtocolFactory(Connection dtc);

        protected ProtocolFactory _protocol;
        private bool _disposed;
        private int _queued;
        //public string BaseUrl { get { return Binds.Path; } set { Binds.SetPath( value ); } }
        
        public Uri EndPoint { get; protected set; }

        ~Listener()
        {
            Dispose(false);
        }

        public Listener()
        {
        }

        protected virtual Connection CreateConnection()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                // Free other state (managed objects).
            }
            _disposed = true;
            StopService();
        }

        // todo -> make extra interface
        public virtual ServiceProtocol BeginService(Connection dtc)
        {
            Task.Run((Action)ListenOn);
            return _protocol(dtc);
        }

        public virtual void EndService(Connection dtc)
        {
        }

        protected void ListenOn()
        {
            //todo : start ListenEx on new task-fiber
            //or may be not as accepts go async soon and return.
            ListenEx(CreateConnection());
        }

        public async void ListenEx(Connection dataConnection)
        {
            // accept inside try so EndService could clean up.
            try
            {
                await dataConnection.AcceptAsync();
                var protocolHandler = BeginService(dataConnection);
                do
                {
                    // process requests from this connection.
                    protocolHandler.BeginRequest();
                    while (protocolHandler.KeepReading)
                    {
                        await dataConnection.ReadAsync();
                        protocolHandler.CheckRequestData();
                    }

                    // execute collected requests.
                    var stillExecuting = protocolHandler.ParseExecuteRequest();
                    while (stillExecuting > 0)
                    {
                        await dataConnection.FlushAsync(false);
                        stillExecuting = await protocolHandler.WaitForExecuting();
                    }
                    // send last piece of accumulated response to the client.
                    await dataConnection.FlushAsync(true);
                } 
                while (dataConnection.KeepAlive);
                
                // allow chance of graceful close.
                await dataConnection.CloseAsync();
            }
            finally
            {
                //todo: pre-close connection on errors ??
                EndService(dataConnection);
            }
        }

        public void Start(DataEncoding dc, string url, int max = 0, int seeds = 0)
        {
            var uri = new Uri(url);
            //IProtocolFactory proto = null;
            switch (dc)
            {
                //case DataEncoding.Proto: proto = ProtocolFactory.PipeRpc; break;
                //case DataEncoding.Json: proto = ProtocolFactory.JsonRpc; break;
                case DataEncoding.Any: break;
                default: throw new NotSupportedException();
            }
            Start(uri, null, seeds);
        }

        public void Start(Uri url, ProtocolFactory protocol, int seeds = 0)
        {
            if (url == null) throw new ArgumentNullException("url");
            _protocol = protocol;
            EndPoint = url;
            try
            {
                StartService();
                // seed initial listener threads.
                seeds = seeds > 0 ? seeds : Environment.ProcessorCount;
                _queued = seeds;
                while (seeds-- > 0) ListenOn();
            }
            catch
            {
                Stop();
                throw;
            }
        }

        public void Start(string url, int seeds = 0)
        {
            if (url == null) throw new ArgumentNullException("url");
            Start(new Uri(url), null);
        }

        public void Stop()
        {
            Dispose();
        }

        protected virtual void StartService() { }
        protected virtual void StopService() { }
    }
}

