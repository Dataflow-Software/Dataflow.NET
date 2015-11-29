/* 
 * Key-Value memcache for .NET, using virtual(non-GC) memory.
 * Supports in-process api and binary memcached protocols over tcp.
 * Optimized for multi-get/pipelined access profile.
 * (c) Viktor Poteryakhin, 2013-2015.
 */

using System;
using Dataflow.Serialization;
using Dataflow.Memcached;

namespace Dataflow.Caching
{
    public unsafe partial class LocalCache
    {
        public const string sCachedVersion = "1.4.15-[dfs]";
        public enum ChangeValue { Set = xSet, Insert = xAdd, Replace = xReplace, Append = xAppend, Prepend = xPrepend }
        private const int iHSubMask = 0x1F, iHSubBits = 5;

        // global lock for cache instance.
        private System.Threading.SpinLock _lock;

        // hash table for key-value lookups.
        protected BitSet64 QuietOps, ReqsAdds, ReqsSpecial;
        protected BitSet64 RespWithExt, RespWithKey, IncDecOps;

        // hash and LRU data fields.
        private DataKey* _lruHead, _lruTail;
        private HashBucket* _htable;
        private uint _flushMark, _flushTime;
        private readonly uint _hashMask;
        private readonly int _hashBits;
        private int _tickTrac;

        // statistics collection and reporting.
        private readonly CachedConfiguration _config;
        public GlobalStats Stats;

        public uint CurrentTime { get { return TimeOrigin + Uptime; } }
        public uint TimeOrigin { get; private set; }
        public uint Uptime { get { return Stats.Uptime; } }

        ~LocalCache()
        {
            Dispose();
        }

        public LocalCache(CachedConfiguration config)
        {
            _config = config;
            _lock = new System.Threading.SpinLock(false);
            _tickTrac = Environment.TickCount;
            TimeOrigin = (uint)Pbs.DateToMsecs(DateTime.UtcNow);
            // init bitmasks for opcodes.
            QuietOps = new BitSet64(xGetQ, xSetQ, xGetKQ, xAddQ, xReplaceQ, xDeleteQ, xIncQ, xDecQ, xFlushQ, xAppendQ, xPrependQ, xQuitQ, xGatQ, xGatKQ);
            ReqsAdds = new BitSet64(xSet, xSetQ, xAdd, xAddQ, xAppend, xAppendQ, xPrepend, xPrependQ, xInc, xIncQ, xDec, xDecQ);
            ReqsSpecial = new BitSet64(xQuit, xQuitQ, xFlush, xVersion, xFlushQ, xStat, xNoop);
            RespWithExt = new BitSet64(xGet, xGetQ, xGetK, xGetKQ);
            RespWithKey = new BitSet64(xGetK, xGetKQ);
            IncDecOps = new BitSet64(xInc, xIncQ, xDec, xDecQ);
            // setup initial memory states.
            _virtual_memory = new WindowsVirtualMemory(config.UseLargePages);
            Stats.Init(config.CacheSize);
            ResetSlabs();
            // calculate hash bits controls.
            _hashBits = 1; _hashMask = _config.HashTableSize - 1;
            for (var i = 1; i < _hashMask; i *= 2) _hashBits++;
            _hashBits -= 1;
            // sign for clock notifications.
            //todo:Clock.System.Append(OnClock, 1000);
        }

        private void EvictForSize(int size)
        {
            SlabList* slix = null;
            var bcnt = 0;
            if (size <= iLastSlabSize)
                slix = _slabs + _slab_id(size);
            else bcnt = 1 + (size - 1) / iBigBlockSize;
            // evict items till enough memory collected.
            for (DataKey* dk = _lruTail, dx; dk != null; dk = dx)
            {
                dx = dk->lrup;
                EvictKey(dk);
                if (slix != null)
                    if (slix->count > 0) break;
                    else continue;
                if (_cntBigs >= bcnt) break;
            }
            if (_lruTail == null)
                throw new NotSupportedException("evict:fail");
            // delete one more tail item, if it is expired.
            if (IsExpired(_lruTail))
                EvictKey(_lruTail);
        }

        private bool OnClock(long uptime, uint elapsed)
        {
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                Stats.UpdateStatistics(uptime, elapsed);
                if (_flushTime != 0 && _flushTime <= Uptime)
                    FlushCache(0);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
            return false;
        }

        private void DeleteKey(DataKey* dk)
        {
            // remove entry from LRU list.
            DataKey* prev = dk->lrup, next = dk->lrux;
            if (next == null)
                if ((_lruTail = prev) != null)
                    prev->lrux = null;
                else _lruHead = null;
            else
            {
                next->lrup = prev;
                if (prev != null) prev->lrux = next;
                else _lruHead = next;
            }
            // free key-mem and release val-mem.
            FreeDk(dk);
        }

        private int FlushCache(uint etime)
        {
            if (etime != 0)
                _flushTime = CalcEtime(etime);
            else
            {
                // update current generation marker.
                _flushMark += 1;
                _flushTime = 0;
                Stats.Flush();
            }
            return iSuccess;
        }

        private uint CalcEtime(uint etime)
        {
            if (etime == 0) return etime;
            return etime > 30 * 24 * 3600 ? etime - TimeOrigin : etime + Uptime;
        }

        private bool IsExpired(DataKey* dk)
        {
            if (dk->flushMark == _flushMark)
            {
                var etime = dk->etime;
                return etime != 0 && etime < Uptime;
            }
            return true;
        }

        private void EvictKey(DataKey* dk)
        {
            LookupKey(dk, true);
            Stats.Evictions++;
            Stats.CurrentItems--;
        }

        private DataKey* LruUpdate(DataKey* cp)
        {
            if (_lruHead != cp)
            {
                DataKey* prev = cp->lrup, lx = cp->lrux;
                prev->lrux = lx;
                if (lx != null) lx->lrup = prev;
                else _lruTail = prev;
                cp->lrup = null; cp->lrux = _lruHead;
                _lruHead->lrup = cp;
                _lruHead = cp;
            }
            return cp;
        }

        private DataKey* LookupKey(DataKey* rqs, bool deleteKey = false)
        {
            var level = 0;
            var he = _htable + (rqs->hash & _hashMask);
            do
            {
                if (he->IsList)
                {
                    DataKey* cp = he->List, nx;
                    for (DataKey* wx = null; cp != null; wx = cp, cp = nx)
                    {
                        nx = cp->next;
                        // check eviction opportunities.
                        if (IsExpired(cp))
                        {
                            var expired = DataKey.Equal(cp, rqs);
                            he->Pop(wx);
                            DeleteKey(cp);
                            if (expired) break;
                            cp = wx;
                            continue;
                        }
                        // check if we have a clean match.
                        if (DataKey.Equal(cp, rqs))
                        {
                            if (!deleteKey)
                                return LruUpdate(cp);
                            he->Pop(wx);
                            DeleteKey(cp);
                            return null;
                        }
                    }
                }
                else
                {
                    var htp = _hashBits + iHSubBits * level++;
                    htp = (int)(rqs->hash >> htp) & iHSubMask;
                    he = he->Htab + htp;
                    continue;
                }
                // if command can append to cache, request entry will be consumed.
                if (!ReqsAdds.Has(rqs->opcode) || deleteKey)
                    return null;
                Stats.CurrentItems++;
                // create new data key for the request.
                var nk = GetDk(rqs->KeySize);
                _copy_with_key(nk, rqs);
                // add new key to the head of LRU list.
                var head = _lruHead;
                nk->lrup = null;
                if (head != null) { nk->lrux = head; head->lrup = nk; }
                else _lruTail = nk;
                var hmax = he->Push(_lruHead = nk);
                if (hmax > 8 && hmax > 8 + level * 2 && level < 3)
                    he->Split(GetBucket(), _hashBits + level * iHSubBits, iHSubMask);
                return nk;
            } while (true);
        }

        protected int CasCounts(DataKey* cit, DataKey* src, bool inc)
        {
            if (cit->CheckCas(src))
                return iNotStored;
            // apply inc/dec logic to key mem buffer only when size == 8.
            long counter = 0;
            if (cit->val_addr != null)
            {
                if (!cit->GetNumeric(ref counter)) return iNotNumeric;
                if (inc)
                    counter += (long)src->longval;
                else
                {
                    counter -= (long)src->longval;
                    if (counter < 0) counter = 0;
                }
            }
            else
            {
                cit->val_addr = GetMem(22);
                counter = src->def_val;
            }
            // extract current counter value for the response.
            cit->SetNumeric(counter);
            src->longval = counter;
            return iSpecialCommand;
        }

        protected void AppendValuesData(DataKey* lkey, DataKey* rkey)
        {
            var left = lkey->val_addr;
            int lsize = lkey->ValSize, rsize = rkey->ValSize;
            var total = lsize + rsize;
            if (total <= iBigBlockSize)
            {
                int lid = _slab_id(lsize), tid = _slab_id(total);
                if (lid != tid)
                {
                    var next = GetMem(total);
                    _memcopy(_skip(next, 0), _skip(left, 0), lsize);
                    ReleaseValue(lkey);
                    lkey->val_addr = left = next;
                }
                lkey->ValSize = total;
                _memcopy(_skip(left, lsize), _skip(rkey->val_addr, 0), rsize);
                return;
            }
            // could be optimized later to skip most of the copying in some common scenarios.
            byte* bp = stackalloc byte[total];
            _memcopy_rc(bp, left, lsize);
            _memcopy_rc(bp + lsize, rkey->val_addr, rsize);
            var bigm = GetMem(total);
            _dbkcopy(bigm, bp, total);
            ReleaseValue(lkey);
            lkey->val_addr = bigm;
            lkey->ValSize = total;
        }

        protected void AppendValues(DataKey* lkey, DataKey* rkey)
        {
            if (rkey->ValSize == 0)
                return;
            if (lkey->ValSize != 0)
            {
                AppendValuesData(lkey, rkey);
                ReleaseValue(rkey);
            }
            else
            {
                lkey->val_addr = rkey->val_addr;
                lkey->ValSize = rkey->ValSize;
                rkey->val_addr = null;
                rkey->ValSize = 0;
            }
        }

        protected int CasAppend(DataKey* dst, DataKey* src, bool append)
        {
            Stats.CountUpdates++;
            if (dst->CheckCas(src))
                return iNotStored;
            if (append)
                AppendValues(dst, src);
            else
            {
                AppendValues(src, dst);
                dst->val_addr = src->val_addr;
                dst->ValSize = src->ValSize;
                src->val_addr = null;
                src->ValSize = 0;
            }
            return iSuccess;
        }

        protected int CasUpdate(DataKey* dst, DataKey* src)
        {
            Stats.CountUpdates++;
            if (dst->CheckCas(src))
                return iNotStored;
            dst->val_addr = src->val_addr;
            dst->size = src->size;
            dst->etime = CalcEtime(src->etime);
            src->val_addr = null;
            src->ValSize = 0;
            return iSuccess;
        }

        protected int GetAndTouch(DataKey* ret, DataKey* cit, bool touch = false)
        {
            if (cit == null)
            {
                Stats.CountMisses++;
                return iKeyNotFound;
            }
            Stats.CountGetHits++;
            ret->cas = cit->cas;
            ret->flags = cit->flags;
            ret->SetValue(cit);
            if (touch)
                ret->etime = CalcEtime(cit->etime);
            return iSuccess;
        }

        protected int SpecOps(DataKey* cop)
        {
            if (cop->opcode == xFlush || cop->opcode == xFlushQ)
                return FlushCache(cop->etime);
            return iSpecialCommand;
        }

        protected int ExecuteRequest(DataKey* cop)
        {
            Stats.RequestCount++;
            int oc = cop->opcode;
            if (ReqsSpecial.Has(oc))
                return SpecOps(cop);
            var deleteKey = (oc == xDelete) || (oc == xDeleteQ);
            var lce = LookupKey(cop, deleteKey);
            var xtag = false;
            switch (cop->opcode)
            {
                case xGet:
                case xGetK:
                case xGetQ:
                case xGetKQ:
                    return GetAndTouch(cop, lce, xtag);
                case xGat:
                case xGatQ:
                case xGatK:
                case xGatKQ:
                    xtag = true; goto case xGet;
                case xAdd:
                case xAddQ:
                    if (lce->cas == 0)
                        return CasUpdate(lce, cop);
                    ReleaseValue(cop);
                    return iKeyExists;
                case xReplace:
                case xReplaceQ:
                    if (lce == null) return iKeyNotFound;
                    goto case xSet;
                case xSet:
                case xSetQ:
                    return CasUpdate(lce, cop);
                case xInc:
                case xIncQ:
                    xtag = true; goto case xDec;
                case xDec:
                case xDecQ:
                    return CasCounts(lce, cop, xtag);
                case xDelete:
                case xDeleteQ:
                    break; // done by del_key flag.
                case xAppend:
                case xAppendQ:
                    xtag = true; goto case xPrepend;
                case xPrepend:
                case xPrependQ:
                    return CasAppend(lce, cop, xtag);
                default: return iUnknownCommand;
            }
            return iSuccess;
        }
    }
}
