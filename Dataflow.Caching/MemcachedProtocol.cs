/* 
 * Key-Value memcache for .NET, using virtual(non-GC) memory.
 * Supports in-process api and binary memcached protocols over tcp.
 * Optimized for multi-get/pipelined access profile.
 * (c) Viktor Poteryakhin, 2013-2015.
*/

using System;
using Dataflow.Serialization;
using Dataflow.Remoting;

namespace Dataflow.Caching
{
    public unsafe partial class LocalCache
    {
        public ServiceProtocol GetServiceProtocol(Connection dtc)
        {
            return new MemcachedProtocol(this, dtc);
        }

        public unsafe class MemcachedProtocol : ServiceProtocol
        {
            private DataStorage _data;
            private Connection _dtc;

            private readonly LocalCache _cache;
            private DataKey* _clist, _clast, _ce;
            private int _parsePos, _stackSize;
            private byte* _cp, _ep, _split;

            public MemcachedProtocol(LocalCache cache, Connection dtc)
            {
                _cache = cache;
                _dtc = dtc;
                _data = dtc.Data;
            }

            private DataKey* GetDk(Packet* rqs)
            {
                var dk = (DataKey*)_cp;
                var ksz = rqs->Keylen;
                // data part will be allocated from table.slabs
                var vsz = rqs->Total - rqs->extlen - ksz;
                _cp += _align8((int)ksz + sizeof(DataKey));
                if (_cp > _ep)
                    throw new InsufficientMemoryException("stack");
                if (vsz == 0)
                    dk->size = ksz;
                else
                {
                    dk->val_addr = _cache.AllocValueMem((int)vsz);
                    dk->size = ksz + (vsz << iKeySizeBits);
                }
                dk->opcode = rqs->opcode;
                dk->opaque = rqs->opaque;
                dk->cas = rqs->Cas;
                dk->next = null;
                return dk;
            }

            public override int BeginRequest()
            {
                KeepReading = true;
                _data.Reset();
                return 0;
            }

            public override int CheckRequestData()
            {
                byte[] bt = null;
                int opc, size = 0, pos = 0;
                // validate network buffers content and detects eos.
                do
                {
                    if (size < SizeofPacket)
                    {
                        size = SizeofPacket;
                        pos = _parsePos;
                        if (pos + size > _data.ContentSize)
                            return 0;
                        bt = _data.Peek(ref size, ref pos);
                    }
                    if (bt[pos] != 0x80) break;
                    // get request key size.
                    var ks = bt[pos + 3] + (bt[pos + 2] << 8);
                    _stackSize += _align8(ks + sizeof(DataKey));
                    opc = bt[pos + 1];
                    // get total_size value.
                    var ts = bt[pos + 11] + ((int)bt[pos + 10] << 8) + ((int)bt[pos + 9] << 16);
                    ts += SizeofPacket;
                    pos += ts; size -= ts;
                    if ((ts += _parsePos) > _data.ContentSize)
                        return 0;
                    _parsePos = ts;
                } 
                while (_cache.QuietOps.Has(opc));
                if (_data.ContentSize == _parsePos)
                {
                    KeepReading = false;
                    return 0;
                }
                return iCorruptStream;
            }

            public override int ParseExecuteRequest()
            {
                if (_stackSize > 512 * 1024)
                    throw new InvalidOperationException("request-too-big");

                // allocate space on stack for request headers and keys.
                _clist = _clast = _ce = null;
                byte* sp = stackalloc byte[_stackSize + iDataKeyLimit];
                _split = _ep = sp + _stackSize;
                _cp = sp;
                
                // parse storage data into request entries.
                var bytesIn = _data.ContentSize;
                ParseRequests(_data);
                if (_ce != null)
                    throw new SerializationException("corrupt memcached byte stream");
                
                // run requests through cache instance, copy results into storage and release values.
                try
                {
                    _cache.ExecuteBatch(_clist, bytesIn);
                    OutputResponse(_data.Reset());
                }
                finally
                {
                    _cache.ReleaseValues(_clist, (uint)_data.ContentSize);
                    _stackSize = 0;
                }
                return 0;
            }

            protected bool SkipResponse(DataKey* dk)
            {
                var status = dk->status;
                if (status == iSuccess)
                    return _cache._quietOks.Has(dk->opcode);
                if (status == iKeyNotFound)
                    return _cache._quietGets.Has(dk->opcode);
                return false;
            }

            protected void OutputResponse(DataStorage dts)
            {
                int valPos = 0, valLeft = 0;
                RefCountPtr* valData = null;
                for (var oe = _clist; oe != null; )
                {
                    var ds = dts.GetSegmentToRead(iBigBlockSize);
                    fixed (byte* bp = ds.Buffer)
                    {
                        byte* cp = bp + ds.Offset + ds.Count, ep = cp + ds.Size;
                        for (; oe != null; oe = oe->next)
                        {
                            if (valLeft == 0)
                            {
                                if (SkipResponse(oe)) continue;
                                // write response header information.
                                var xp = WritePacket(oe, cp, ep);
                                // check for boundary crossing in buffer.
                                if (xp == cp) break;
                                cp = xp;
                                if (oe->status != iSuccess) continue;
                                valLeft = oe->ValSize;
                                if (valLeft == 0) continue;
                                valData = oe->val_addr;
                                // quick path, small value copy to output.
                                if (valLeft < iBigBlockSize)
                                {
                                    var bleft = (int)(ep - cp);
                                    if (bleft >= valLeft)
                                    {
                                        _memcopy(cp, _skip(valData, 0), valLeft);
                                        cp += valLeft;
                                        valLeft = 0;
                                        continue;
                                    }
                                }
                                valPos = 0;
                            }
                            // copy value bytes into the buffer.
                            do
                            {
                                int dleft = (int)(ep - cp), sleft = iBigBlockSize - valPos;
                                if (sleft > valLeft) sleft = valLeft;
                                if (dleft <= sleft)
                                {
                                    _memcopy(cp, _skip(valData, valPos), dleft);
                                    valLeft -= dleft;
                                    valPos += dleft;
                                    cp += dleft;
                                    break;
                                }
                                _memcopy(cp, _skip(valData, valPos), sleft);
                                cp += sleft;
                                valLeft -= sleft;
                                if (valLeft == 0) break;
                                valData = valData->Next;
                                valPos = 0;
                            } while (true);
                            // may need to switch the page.
                            if (valLeft != 0)
                                break;
                        }
                        // "commit" generated memcached responses.
                        var bytes = (int)(cp - bp) - ds.Offset;
                        if (bytes > 0)
                            dts.ReadCommit(bytes);
                    }
                }
            }

            private byte* SaveIntoSplit(byte* cp, int left)
            {
                if (left > 0)
                    _memcopy(_split, cp, _parsePos = left);
                return null;
            }

            private byte* ParsePacket(byte* cp, byte* ep)
            {
                var left = (int)(ep - cp);
                if (left < SizeofPacket)
                    return SaveIntoSplit(cp, left);
                var cr = (Packet*)cp;
                if (cr->magic != 0x80)
                    throw new SerializationException("magic byte");
                if (_ce == null)
                {
                    // create new datakey and add it to the list.
                    _ce = GetDk(cr);
                    if (_clist == null) _clist = _ce;
                    else _clast->next = _ce;
                    _clast = _ce;
                }
                int xsz = cr->extlen, xks = xsz + _ce->KeySize;
                // buffer may split on extras or key part.
                if (xks > left - SizeofPacket)
                    return SaveIntoSplit(cp, left);
                // copy key and extras bytes into place.
                cp += sizeof(Packet);
                _set_key(_ce, cp + xsz);
                if (xsz > 0) _ce->SetExtras(cp, xsz);
                return cp + xks;
            }

            private void ParseRequests(DataStorage dts)
            {
                int dataLeft = 0, skip = 0;
                for (_parsePos = 0; true; dts.CommitSentBytes())
                {
                    var ds = dts.GetSegmentToSend();
                    if (ds == null) break;
                    int cnt = ds.Count, offset = ds.Offset;
                    // may need to skip some bytes if packet splits.
                    if (skip != 0)
                    {
                        if (skip >= cnt) { skip -= cnt; continue; }
                        cnt -= skip; offset += skip;
                        skip = 0;
                    }
                    // process storage buffer/page.
                    fixed (byte* bp = ds.Buffer)
                    {
                        byte* cp = bp + offset, ep = cp + cnt;
                        if (dataLeft > 0)
                        {
                            // append bytes to "open" value.
                            var sz = Math.Min(dataLeft, (int)(ep - cp));
                            _ce->AppendValue(cp, sz, _ce->ValSize - dataLeft);
                            if (sz == dataLeft)
                            {
                                cp += sz; dataLeft = 0;
                                _ce = null;
                            }
                            else { cp = ep; dataLeft -= sz; }
                        }
                        while ((cp = ParsePacket(cp, ep)) != null)
                        {
                            // quick path, no page breaks.
                            var vsz = _ce->ValSize;
                            if (vsz > 0)
                            {
                                var left = vsz - (int)(ep - cp);
                                if (left > 0)
                                {
                                    dataLeft = left;
                                    _ce->AppendValue(cp, vsz - left, 0);
                                    break;
                                }
                                _ce->AppendValue(cp, vsz, 0);
                                cp += vsz;
                            }
                            // indicates that request is fully built.
                            _ce = null;
                        }
                        if (cp == null && _parsePos > 0)
                        {
                            // read "split" request into continuos area[].
                            int tsz = iDataKeyLimit - _parsePos, lsz = tsz, pos = ds.Count;
                            var bt = dts.Peek(ref tsz, ref pos);
                            if (lsz < tsz) tsz = lsz;
                            _memcopy(_split + _parsePos, bt, pos, tsz);
                            tsz += _parsePos;
                            var nx = ParsePacket(_split, _split + tsz);
                            if (nx == null)
                                throw new InvalidOperationException("eos");
                            skip = (int)(nx - _split) - _parsePos;
                            dataLeft = _ce->ValSize;
                            if (dataLeft == 0) _ce = null;
                            _parsePos = 0;
                        }
                    }
                }
            }

            private byte* WriteSpecial(DataKey* dk, Packet* hr, byte* ep)
            {
                var bp = (byte*)hr;
                var cp = bp + SizeofPacket;
                switch (dk->opcode)
                {
                    case xInc:
                    case xIncQ:
                    case xDec:
                    case xDecQ:
                        hr->Total = 8;
                        var ul = dk->longval;
                        for (var i = 56; i >= 0; i -= 8) *(cp++) = (byte)(ul >> i);
                        break;
                    case xVersion:
                        cp = WriteSingleStat(bp, ep, "version");
                        break;
                    case xStat:
                        cp = dk->KeySize == 0 ? WriteStats(bp, ep) : WriteSingleStat(bp, ep, _get_key(dk));
                        break;
                    case xNoop:
                        break;
                    case xQuit:
                    case xQuitQ:
                        _dtc.KeepAlive = false;
                        break;
                    // those commands are processed inside cache execute-request lock.
                    case xFlush:
                    case xFlushQ:
                        break;
                }
                hr->status = 0;
                return cp;
            }

            private byte* WritePacket(DataKey* dk, byte* cp, byte* ep)
            {
                var space = (int)(ep - cp) - sizeof(Packet);
                if (space < 0) return cp;
                // fill header memory block(24 bytes) with 0's.
                var zp = (ulong*)cp; zp[0] = 0; zp[1] = 0; zp[2] = 0;
                var hr = (Packet*)cp;
                hr->magic = 0x81;
                hr->opaque = dk->opaque;
                hr->Cas = dk->cas;
                int status = dk->status;
                hr->status = (byte)status;
                // handle errors and special commands branch.
                if (status != 0)
                {
                    hr->opcode = dk->opcode;
                    if (status == iSpecialCommand)
                        return WriteSpecial(dk, hr, ep);
                    // is error code enough, or we should add error message later ??
                    return (byte*)(hr + 1);
                }
                ep = (byte*)(hr + 1);
                int opc = dk->opcode, total = 0;
                hr->opcode = (byte)opc;
                if (_cache.RespWithExt.Has(opc))
                {
                    if (space < 4) return cp;
                    // flags are stored in BE order.
                    hr->extlen = 4;
                    total += 4;
                    *(uint*)ep = dk->flags;
                    ep += 4;
                }
                if (_cache.RespWithKey.Has(opc))
                {
                    var ksz = dk->KeySize;
                    if (space < ksz) return cp;
                    total += ksz;
                    _memcopy(ep, (byte*)(dk + 1), ksz);
                    hr->Keylen = (uint)ksz;
                    ep += ksz;
                }
                hr->Total = (uint)(total + dk->ValSize);
                return ep;
            }

            #region -- statistics output.

            private static readonly string[] StatsNames = new[] { 
                "pid", "uptime", "time", "version", "curr_items", 
                "get_hits", "get_misses", "evictions", "limit_maxbytes",
                "--" // closing(unknown) stats name.
            };

            protected byte* WriteAnsiString(byte* cp, string s)
            {
                foreach (var c in s) *cp++ = (byte)c;
                return cp;
            }

            protected byte* WriteSingleStat(byte* cp, byte* ep, string name)
            {
                // should never really happen, but still handled.
                if ((int)(ep - cp) - sizeof(Packet) - iMaxStatSize < 0)
                    throw new SerializationException("stats overflow.");
                var pkt = (Packet*)cp;
                *pkt = new Packet();
                pkt->magic = 0x81;
                pkt->opcode = xStat;
                pkt->status = iSuccess;
                var ksz = (uint)name.Length;
                pkt->Keylen = ksz;
                cp = WriteAnsiString((byte*)(pkt + 1), name);
                switch (name)
                {
                    case "pid": ep = WriteAnsiString(cp, _cache.Stats.ProcessId.ToString()); break;
                    case "uptime": ep = WriteAnsiString(cp, _cache.Uptime.ToString()); break;
                    case "time": ep = WriteAnsiString(cp, _cache.CurrentTime.ToString()); break;
                    case "version": ep = WriteAnsiString(cp, sCachedVersion); break;
                    case "curr_items": ep = WriteAnsiString(cp, _cache.Stats.CurrentItems.ToString()); break;
                    case "get_hits": ep = WriteAnsiString(cp, _cache.Stats.CountGetHits.ToString()); break;
                    case "get_misses": ep = WriteAnsiString(cp, _cache.Stats.CountMisses.ToString()); break;
                    case "evictions": ep = WriteAnsiString(cp, _cache.Stats.Evictions.ToString()); break;
                    case "limit_maxbytes": ep = WriteAnsiString(cp, _cache.Stats.MemoryLimit.ToString()); break;
                    default: pkt->Keylen = pkt->Total = 0; return (byte*)pkt;
                }
                pkt->Total = (uint)(ep - cp) + ksz;
                return ep;
            }

            protected byte* WriteStats(byte* cp, byte* ep)
            {
                var nxt = cp;
                try
                {
                    foreach (var s in StatsNames)
                    {
                        nxt = WriteSingleStat(cp, ep, s);
                        if (nxt == cp) break;
                        cp = nxt;
                    }
                }
                catch (SerializationException)
                {
                    var pkt = (Packet*)nxt;
                    pkt->Keylen = pkt->Total = 0;
                }
                return nxt + sizeof(Packet); ;
            }

            #endregion
        }
    }
}

