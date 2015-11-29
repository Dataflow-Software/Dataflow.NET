/* 
 * Key-Value memcache for .NET, using virtual(non-GC) memory.
 * Supports in-process api and binary memcached protocols over tcp.
 * Optimized for multi-get/pipelined access profile.
 * (c) Viktor Poteryakhin, 2013-2015.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using Dataflow.Serialization;
using Dataflow.Memcached;
using Dataflow.Remoting;

namespace Dataflow.Caching
{
    public unsafe partial class LocalCache : IChannelSync, IChannel, ICachedClient
    {
        private static byte[] sLocalVersion = System.Text.Encoding.UTF8.GetBytes(sCachedVersion + "-local");
        private BitSet64
            _quietOks = new BitSet64(xSetQ, xAddQ, xAppendQ, xReplaceQ, xPrependQ, xFlushQ, xDeleteQ, xQuitQ),
            _quietGets = new BitSet64(xGetQ, xGetKQ, xGatQ, xGatKQ);

        public const int xGet = 0, xSet = 1, xAdd = 2, xReplace = 3, xDelete = 4, xInc = 5, xDec = 6, xQuit = 7,
            xFlush = 8, xGetQ = 9, xNoop = 0xA, xVersion = 0xB, xGetK = 0xC, xGetKQ = 0xD,
            xAppend = 0xE, xPrepend = 0xF, xStat = 0x10, xSetQ = 0x11, xAddQ = 0x12, xReplaceQ = 0x13,
            xDeleteQ = 0x14, xIncQ = 0x15, xDecQ = 0x16, xQuitQ = 0x17, xFlushQ = 0x18,
            xAppendQ = 0x19, xPrependQ = 0x1A, xTouch = 0x1C, xGat = 0x1D, xGatQ = 0x1E,
            xGatK = 0x23, xGatKQ = 0x24;

        public const int
            iSuccess = 0, iKeyNotFound = 1, iKeyExists = 2, iValueTooBig = 3, iInvalidArgs = 4,
            iNotStored = 5, iNotNumeric = 6, iUnknownCommand = 0x81, iOutOfMemory = 0x82,
            iCorruptStream = 0x9E, iSpecialCommand = 0xFF,
            iKeySizeBits = 11, iKeySizeMask = 0x7FF,
            iDataKeyLimit = iLastSlabSize, iMaxStatSize = 45;

        #region implemented interface bindings

        CachedResponse ICachedClient.Execute(CachedRequest request)
        {
            var batch = new CachedRequest[1] { request };
            var result = new CachedResponse[1] { new CachedResponse(request) };
            ExecuteBatch(batch, result);
            return result[0];
        }

        CachedResponse[] ExecuteBatch(CachedRequest[] batch)
        {
            var response = new CachedResponse[batch.Length];
            for (int i = 0; i < batch.Length; i++)
                response[i] = new CachedResponse(batch[i]);
            ExecuteBatch(batch, response);
            return response;
        }

        void IChannelSync.Dispatch(Message request, Message response)
        {
            var batch = new CachedRequest[1] { request as CachedRequest };
            var result = new CachedResponse[1] { response as CachedResponse };
            ExecuteBatch(batch, result);
        }

        void IChannel.DispatchAsync(Request request)
        {
            ExecuteAsyncRequest(request);
        }

        private void ExecuteAsyncRequest(Request request)
        {
            try
            {
                ExecuteBatch(new CachedRequest[1] { (CachedRequest)request.Params }, new CachedResponse[1] { (CachedResponse)request.Result });
                request.Complete();
            }
            catch (Exception ex)
            {
                request.Fail(new Signal.Runtime(ex));
            }
        }

        #endregion

        private int CalcStackStize(CachedRequest[] batch)
        {
            var bsize = 0;
            foreach(var request in batch)
            {
                var key = request.Key;
                bsize += _align8(sizeof(DataKey) + (key == null ? 0 : key.Length));
            }
            return bsize;
        }

        private void ExecuteBatch(DataKey* clist, int inBytes)
        {
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                Stats.BytesIn += (ulong)inBytes;
                // execute prepared datakeys(requests).
                for (var ls = clist; ls != null; ls = ls->next)
                    ls->status = (byte)ExecuteRequest(ls);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
        }

        private DataKey* SetDataKey(DataKey* dk, CachedRequest rqs)
        {
            dk->cas = rqs.Cas;
            dk->etime = rqs.Expires;
            dk->flags = Pbs.SwapBytes((uint)rqs.Flags);
            if (!IncDecOps.Has(dk->opcode))
            {
                var data = rqs.Data;
                var size = rqs.DataCount;
                if (size > 0)
                {
                    dk->ValSize = size;
                    dk->val_addr = AllocValueMem(size);
                    fixed (byte* ds = data)
                        dk->AppendValue(ds + rqs.DataOffset, size, 0);
                }
            }
            else
            {
                dk->longval = (long)rqs.Delta;
                dk->def_val = (long)rqs.Flags;
            }
            return dk;
        }

        public void UpdateUptime()
        {
            var new_tt = Environment.TickCount;
            var elapsed = new_tt - _tickTrac;
            _tickTrac = new_tt;
            OnClock(Uptime + elapsed, (uint)elapsed);
        }

        public void ExecuteBatch(CachedRequest[] batch, CachedResponse[] result)
        {
            if (batch == null || batch.Length == 0) 
                return;

            // calculate transient memory space requirements.
            var bsize = CalcStackStize(batch);
            uint opaque = 1;
            DataKey* dataKey = null, last = null;
            var db = stackalloc byte[bsize];
            var cp = db;

            // copy requests data into memcached protocol format.
            foreach(var request in batch)
            {
                var dk = (DataKey*)cp;
                _init_dk(dk, request.Opcode);
                var key = request.Key;
                if (key != null)
                {
                    _set_key(dk, key, key.Length);
                    cp += _align8(sizeof(DataKey) + key.Length);
                }
                else cp += sizeof(DataKey);
                dk->opaque = opaque++;
                dk = SetDataKey(dk, request);
                // append new request to dataKey list
                if (last == null) dataKey = dk;
                else last->next = dk;
                last = dk;
            }

            // execute built-up request chain against cache.
            ExecuteBatch(dataKey, 0);
            
            // move cached results into responses fields.
            foreach(var response in result)
            {
                // copy response data from datakey into message.
                var status = dataKey->status;
                response.Status = 0;
                switch (status)
                {
                    case 0:
                        response.Cas = dataKey->cas;
                        response.Flags = Pbs.SwapBytes(dataKey->flags);
                        var dtsize = dataKey->ValSize;
                        if (dtsize > 0)
                        {
                            response.Data = new byte[dtsize];
                            _memcopy(response.Data, dataKey->val_addr, dtsize);
                        }
                        break;
                    case iSpecialCommand:
                        if (IncDecOps.Has(dataKey->opcode))
                        {
                            response.Cas = dataKey->cas;
                            response.Counter = (ulong)dataKey->longval;
                        }
                        else
                        {
                            //todo: support for more stats.
                            if (dataKey->opcode == (byte)Opcode.Version)
                                response.Data = sLocalVersion;
                        }
                        break;
                    default:
                        response.Status = status;
                        break;
                }
                // advance datakey in sync with responses list.
                dataKey = dataKey->next;
            }
        }

        public void GetStatistics(CachedStats cs)
        {
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                Stats.CopyTo(cs);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
        }
    }
}
