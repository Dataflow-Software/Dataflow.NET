using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.IO;

using Dataflow.Serialization;
using Dataflow.Memcached;
using Dataflow.Remoting;
using Dataflow.Caching;

namespace Cached.Net
{
    public class ServiceHost
    {
        private Listener _net;

        public CachedConfiguration Config { get; private set; }
        public LocalCache Cache { get; private set; }

        public void Start(bool console, string[] args)
        {
            //--load configuration from json file;
            var cjson = File.ReadAllText("cached.config");
            Config = new CachedConfiguration();
            Config.MergeFrom(cjson);
            
            //-- create instance of local cache and bind network listener to it.
            Cache = new LocalCache(Config);
            var uri = new Uri(Config.Address);
            switch (uri.Scheme)
            {
                case "tcp": _net = new SocketListener(); break;
                //--case "pipe": _net = new NamedPipeListener(); break;
                default: throw new ArgumentException("address:schema");
            }
            _net.Start(uri, Cache.GetServiceProtocol);
        }

        public void Stop()
        {
            if (Cache == null) return;
            try { if (_net != null) _net.Stop(); }
            catch { }
            _net = null;
            Cache.Dispose();
            Cache = null;
        }
    }
}
