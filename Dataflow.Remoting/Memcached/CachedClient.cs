using System;
using System.Threading;
using System.Threading.Tasks;
using Dataflow.Serialization;
using Dataflow.Remoting;

namespace Dataflow.Memcached
{
    public interface ICachedClient
    {
        CachedResponse Execute(CachedRequest request);
        //CachedResponse[] Execute(CachedRequest[] batch);
    }

    public class CachedClient : ServiceClient, ICachedClient
    {
        private bool _checkStatus;

        public CachedClient(IChannel channel, bool checkStatus = true)
            : base(channel, null)
        {
            _checkStatus = checkStatus;
        }

        public CachedClient(string url, int maxConnections = 1, bool checkStatus = false)
            : base(new RemoteTunnel(url, DataEncoding.Memcached, maxConnections), null)
        {
            _checkStatus = checkStatus;
        }

        #region sync api - classic style memcached methods.

        public CachedResponse Execute(CachedRequest request)
        {
            var response = new CachedResponse(request);
            ChannelSync.Dispatch(request, response);
            if (_checkStatus && response.Status != 0)
                throw new InvalidOperationException("cache-operation-status: " + response.Status);
            return response;
        }

        public CachedResponse[] Execute(BatchRequest batch)
        {
            var args = batch.Params;
            var rets = new CachedResponse[args.Length];
            for (int i = 0; i < args.Length; i++)
            {
                rets[i] = new CachedResponse(args[i]);
            }
            //ChannelSync.DispatchBatch(args, rets);
            batch.CompleteSync(rets);
            return rets;
        }

        public CachedResponse Add(string key, byte[] data, uint expire = 0) { return Execute(new CachedRequest(Opcode.Add, key, data) { Expires = expire }); }
        public CachedResponse Append(string key, byte[] data, ulong cas = 0) { return Execute(new CachedRequest(Opcode.Append, key, data) { Cas = cas }); }
        public CachedResponse Prepend(string key, byte[] data, ulong cas = 0) { return Execute(new CachedRequest(Opcode.Prepend, key, data) { Cas = cas }); }
        public ulong Dec(string key, long amount, long defv, int expire = 0) { return Execute(CachedRequest.Inc(key, amount, defv, true)).Counter; }
        public CachedResponse Delete(string key) { return Execute(new CachedRequest(Opcode.Delete, key)); }
        public void Flush(uint delay) { Execute(new CachedRequest(Opcode.Flush) { Expires = delay }); }
        public byte[] Get(string key) { return Execute(new CachedRequest(Opcode.Get, key)).Data; }
        public string GetText(string key) { return Execute(new CachedRequest(Opcode.Get, key)).Text; }
        public CachedResponse GetCas(string key) { return Execute(new CachedRequest(Opcode.Get, key)); }
        public ulong Inc(string key, long amount, long defv, uint expire = 0) { return Execute(CachedRequest.Inc(key, amount, defv)).Counter; }
        public CachedResponse Replace(string key, byte[] data, uint expire = 0) { return Execute(new CachedRequest(Opcode.Replace, key, data) { Expires = expire }); }
        public CachedResponse Set(string key, byte[] data, uint expire = 0, ulong cas = 0) { return Execute(new CachedRequest(Opcode.Set, key, data) { Expires = expire, Cas = cas }); }
        public string Version() { return Execute(new CachedRequest(Opcode.Version)).Text; }

        #endregion

        #region async api - await based cache interface.

        public AwaitCachedRequest ExecuteAsync(CachedRequest args)
        {
            var awaitable = new AwaitCachedRequest();
            var request = new Request(awaitable, args, awaitable.Result, null);
            Channel.DispatchAsync(request);
            return awaitable;
        }

        public BatchRequest ExecuteAsync(BatchRequest batch)
        {
            var ls = batch.GetRequests();
            var args = batch.Params;
            for (int i = 0; i < ls.Length; i++)
            {
                var request = ls[i];
                if (ClientProtocol.QuietMap[args[i].Opcode] == Opcode.Unused)
                    throw new ArgumentException("batch contain invalid command: " + args[i].Opcode);
                Channel.DispatchAsync(request);
            }
            return batch;
        }

        public AwaitCachedRequest GetAsync(string key)
        {
            return ExecuteAsync(new CachedRequest(Opcode.Get, key));
        }

        public AwaitCachedRequest SetAsync(string key, byte[] data, uint expire = 0, ulong cas = 0)
        {
            return ExecuteAsync(new CachedRequest(Opcode.Set, key, data) { Expires = expire, Cas = cas });
        }

        #endregion <async api>.

        public class AwaitCachedRequest : AwaitableRequest<CachedResponse>
        {
            public AwaitCachedRequest() { } 
        }

        public class BatchRequest : AwaitableBatch<CachedResponse, CachedRequest>
        {
            #region "convenience" parameter formatting methods

            public CachedRequest Delete(string key) { return AddRequest(new CachedRequest(Opcode.Delete, key)); }
            public CachedRequest Get(string key) { return AddRequest(new CachedRequest(Opcode.Get, key)); }
            public CachedRequest Set(string key, byte[] data, uint expire = 0, ulong cas = 0) { return AddRequest(new CachedRequest(Opcode.Set, key, data) { Expires = expire, Cas = cas }); }

            #endregion
        }
    }
}