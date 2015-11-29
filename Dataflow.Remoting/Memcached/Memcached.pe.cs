using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Dataflow.Serialization;

namespace Dataflow.Memcached
{
    public partial class CachedRequest
    {
        public const int MAX_KEY_LENGTH = 64000;
        public const int MAX_DATA_LENGTH = 0x100000; // 1MB per memcached spec.

        public int DataCount { get; set; }  // todo: overdoing it ???
        public int DataOffset { get; set; } // -----

        public CachedRequest() { }

        [DebuggerStepThrough]
        public CachedRequest(Memcached.Opcode opc)
        {
            Opcode = (int)opc;
        }

        [DebuggerStepThrough]
        public CachedRequest(Memcached.Opcode opc, string skey, byte[] bdata = null)
        {
            Opcode = (int)opc;
            if (!string.IsNullOrEmpty(skey))
                Key = ValidateKey(skey);
            Data = bdata;
            if (bdata != null)
                DataCount = bdata.Length;
        }
        [DebuggerStepThrough]
        public CachedRequest(Memcached.Opcode opc, byte[] bkey = null, byte[] bdata = null)
        {
            Opcode = (int)opc;
            Key = ValidateKey(bkey);
            Data = bdata;
            if (bdata != null)
                DataCount = bdata.Length;
        }

        [DebuggerStepThrough]
        public CachedRequest(Memcached.Opcode opc, byte[] bkey, byte[] bdata, int offset, int count)
        {
            Opcode = (int)opc;
            Key = ValidateKey(bkey);
            Data = bdata;
            DataOffset = offset;
            DataCount = count;
        }

        public static byte[] ValidateKey(string skey)
        {
            if (string.IsNullOrEmpty(skey)) throw new ArgumentNullException();
            return ValidateKey(Encoding.UTF8.GetBytes(skey));
        }

        public static byte[] ValidateKey(byte[] bts)
        {
            if (bts.Length > MAX_KEY_LENGTH)
                throw new ArgumentException("cache key is too long: " + bts.Length);
            foreach (var b in bts)
                if (b <= ' ') throw new ArgumentException("cache key contains invalid characters");
            return bts;
        }

        public static CachedRequest Inc(string key, long amount, long defv, bool decr = false)
        {
            return new CachedRequest(decr ? Memcached.Opcode.Dec : Memcached.Opcode.Inc, key) { Flags = (ulong)defv, Delta = (ulong)amount };
        }
    }

    public partial class CachedResponse
    {
        public CachedRequest Request { get; private set; }

        public CachedResponse() { }
        public CachedResponse(CachedRequest request)
        {
            Request = request;
        }

        public string GetStatusText()
        {
            switch ((Memcached.Status)Status)
            {
                case Memcached.Status.Success: return null;
                case Memcached.Status.KeyNotFound: return "key not found";
                case Memcached.Status.KeyExists: return "key exists";
                case Memcached.Status.ValueTooBig: return "value too big";
                case Memcached.Status.InvalidArgs: return "invalid arguments";
                case Memcached.Status.NotStored: return "not stored";
                case Memcached.Status.NotNumeric: return "not a numeric";
                case Memcached.Status.StillExecuting: return "still executing";
                case Memcached.Status.UnknownCommand: return "unknown command";
                case Memcached.Status.OutOfMemory: return "out of memory";
                default: return "unknown error";
            }
        }
        public string Text { get { return Data != null ? Encoding.UTF8.GetString(Data, 0, Data.Length) : null; } }
    }
}
