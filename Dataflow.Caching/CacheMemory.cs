/* 
 * Key-Value memcache for .NET, using virtual(non-GC) memory.
 * Supports in-process api and binary memcached protocols over tcp.
 * Optimized for multi-get/pipelined access profile.
 * (c) Viktor Poteryakhin, 2013-2015.
 */

using System;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace Dataflow.Caching
{
    public unsafe partial class LocalCache
    {
        private const int
            SizeofPacket = 24, SizeofRcp = 8,
            iQuantum = 32, iQuantumShift = 5,
            iLastSlabSize = 2048 - SizeofRcp,
            iBigBlockSize = 3072 - SizeofRcp,
            iExtSizeLimit = 40, iLastSize = iLastSlabSize + SizeofRcp;

        private const ulong iQuantumMask = ~((ulong)iQuantum - 1);

        public interface IVirtualMemory
        {
            void* Alloc(uint numBytes, out uint actualBytes);
            bool Free(void* address, uint numBytes);
            uint PageSize();
        }

        [StructLayout(LayoutKind.Explicit)]
        public unsafe struct RefCountPtr
        {
            [FieldOffset(0)]
            public ulong _data;
            [FieldOffset(0)]
            public RefCountPtr* _list;
            public RefCountPtr* Next { get { return (RefCountPtr*)(_data & 0x0000FFFFFFFFFFFF); } }
            public uint RefCount { get { return (uint)(_data >> 48); } }
            public void AddRef() { _data += 0x1000000000000; }
            public uint Release()
            {
                var rc = (uint)(_data >> 48);
                if (rc != 0) _data -= 0x1000000000000;
                return rc;
            }
        }

        // slab-type memory allocator for cache.
        // uses large VM pages, when OS privileges are properly granted.
        private class LargePage
        {
            private IVirtualMemory _memory;
            public LargePage next;
            public byte* data;
            public uint size;

            public LargePage(IVirtualMemory memory, uint count = 0x800000)
            {
                _memory = memory;
                if (count < 512) count *= 1024 * 1024;
                data = (byte*)_memory.Alloc(count, out size); //-READ|WRITE
                if (data == null)
                    throw new InvalidOperationException("virtual alloc");
            }

            public LargePage Release()
            {
                if (data != null) 
                    _memory.Free(data, size); //-RELEASE
                return null;
            }
        }

        protected struct SlabList
        {
            public RefCountPtr* next;
            public int count;

            public void Push(RefCountPtr* bp)
            {
                bp->_list = next;
                next = bp;
                count++;
            }

            public RefCountPtr* Pop()
            {
                var rc = next;
                if (rc != null)
                {
                    next = rc->_list;
                    rc->_data = 0;
                    count--;
                }
                return rc;
            }

            public void Set(RefCountPtr* ls, int cnt)
            {
                next = ls; 
                count = cnt;
            }
        }

        // slab storage fields.
        private IVirtualMemory _virtual_memory;
        private LargePage _lpBase, _lpHash;
        private LargePage _lpList, _lpNext;
        private HashBucket* _hb_cp, _hb_ep;
        private RefCountPtr* _sbigs;
        private SlabList* _slabs;
        private byte* _cp, _ep;
        private int* _stabs;
        private int _cntBigs, _cntPages;
        private int _cbuckets;

        private byte* _align(byte* bp) { return (byte*)((ulong)(bp + iQuantum - 1) & iQuantumMask); }
        private int _slab_id(int size) { return _stabs[(size - 1) >> iQuantumShift] & 0xFFFF; }
        private int _slab_max(int size) { return _stabs[(size - 1) >> iQuantumShift] >> 16; }
        private int _dk_size(int key) { return key + sizeof(DataKey) - SizeofRcp; }

        protected RefCountPtr* AllocValueMem(int size)
        {
            RefCountPtr* ret;
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                ret = GetMem(size);
            }
            finally
            {
                if(lockTaken) _lock.Exit();
            }
            return ret;
        }

        protected void ReleaseValues(DataKey* clist, uint bytesOut)
        {
            var ls = clist;
            for (; ls != null; ls = ls->next)
                if (ls->ValSize > 0) break;
            // quick check if lock actually needed.
            if (ls == null) return;
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                for (; ls != null; ls = ls->next)
                    ReleaseValue(ls);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
        }

        private RefCountPtr* GetNewMem(int smax)
        {
            var left = (int)(_ep - _cp);
            if (left >= smax)
            {
                var bt = _cp; _cp += smax;
                return (RefCountPtr*)bt;
            }
            // drop "leftover" chunk into proper slab.
            if (left > iQuantum)
            {
                var sx = left >= iLastSize ? iLastSize + 1 : left;
                var ix = _slab_id(sx) - 1;
                _slabs[ix].Push((RefCountPtr*)_cp);
                _cp = _ep;
            }
            // check if some large pages are left to chop.
            var lpage = _lpNext;
            if (_config.ReserveMemory)
                _lpNext = lpage != null ? lpage.next : null;
            else if (_cntPages > 0)
            {
                _cntPages--;
                _lpList = lpage = new LargePage(_virtual_memory, _config.AllocPageSize) { next = _lpList };
            }
            if (lpage != null)
            {
                _cp = lpage.data;
                _ep = _cp + lpage.size;
                return GetNewMem(smax);
            }
            // if all memory is in use, start cache evictions.
            smax -= SizeofRcp;
            EvictForSize(smax);
            return GetMem(smax);
        }

        private RefCountPtr* GetBig(int size = iBigBlockSize)
        {
            RefCountPtr* db = _sbigs, cp = db;
            for (; cp != null; _cntBigs--, cp = cp->_list)
                if ((size -= iBigBlockSize) <= 0)
                {
                    _sbigs = cp->_list;
                    cp->_list = null;
                    _cntBigs--;
                    return db;
                }
            Debug.Assert(_cntBigs == 0, "big-mem");
            _sbigs = null;
            do
            {
                var np = GetNewMem(iBigBlockSize + SizeofRcp);
                np->_list = db; db = np;
            } while ((size -= iBigBlockSize) > 0);
            return db;
        }

        private HashBucket* GetBucket()
        {
            var bucket = _hb_cp;
            _hb_cp = bucket + (iHSubMask + 1);
            if (_hb_cp <= _hb_ep)
            {
                _cbuckets++;
                return bucket;
            }
            uint hbSize;
            if (_cbuckets < 2048)
            {
                _hb_cp = (HashBucket*)GetBig();
                hbSize = iBigBlockSize + SizeofRcp;
            }
            else
            {
                var hash = new LargePage(_virtual_memory, 4) { next = _lpHash };
                hbSize = hash.size;
                _lpHash = hash;
                _hb_cp = (HashBucket*)hash.data;
            }
            _hb_ep = (HashBucket*)((byte*)_hb_cp + hbSize);
            return GetBucket();
        }

        private RefCountPtr* GetMem(int size)
        {
            if (size <= iLastSlabSize)
            {
                size += SizeofRcp;
                var ret = _slabs[_slab_id(size)].Pop();
                return ret != null ? ret : GetNewMem(_slab_max(size));
            }
            return GetBig(size);
        }

        private void FreeMem(RefCountPtr* bp, int size)
        {
            if (size <= iLastSlabSize)
                _slabs[_slab_id(size + SizeofRcp)].Push(bp);
            else
            {
                var ls = bp->Next;
                bp->_list = _sbigs; _sbigs = bp; _cntBigs++;
                if (ls == null) return;
                for (_cntBigs++, bp = ls; bp->_list != null; bp = bp->_list) _cntBigs++;
                bp->_list = _sbigs; _sbigs = ls;
            }
        }

        private DataKey* GetDk(int key)
        {
            // keys values use extra space after struct size for storage.
            var nk = (DataKey*)GetMem(_dk_size(key));
            nk->flushMark = _flushMark;
            nk->val_addr = null;
            nk->options = 0;
            return nk;
        }

        private void FreeDk(DataKey* ce)
        {
            ReleaseValue(ce);
            FreeMem((RefCountPtr*)ce, _dk_size(ce->KeySize));
        }

        private void ReleaseValue(DataKey* dk)
        {
            var vsz = dk->ValSize;
            if (vsz == 0) return;
            var rc = dk->val_addr;
            if (rc->Release() == 0) FreeMem(rc, vsz);
            dk->ValSize = 0; dk->val_addr = null;
        }

        public void Dispose()
        {
            if (_lpBase != null) _lpBase = _lpBase.Release();
            if (_lpHash != null) _lpHash = _lpHash.Release();
            while (_lpList != null)
            {
                var next = _lpList.next;
                _lpList.Release();
                _lpList = next;
            }
            GC.SuppressFinalize(this);
        }

        private void ResetSlabs()
        {
            _cbuckets = 0;
            // hash bucket extra mem pages are returned to system on reset.
            if (_lpHash != null)
            {
                for (var ls = _lpHash; ls != null; ls = ls.next)
                    ls.Release();
                _lpHash = null;
            }
            if (_lpBase == null)
                _lpBase = new LargePage(_virtual_memory, 4);
            _htable = (HashBucket*)_lpBase.data;
            // init shared tables with clean values.
            for (var i = 0; i < _config.HashTableSize; i++)
                _htable[i] = new HashBucket();
            var bp = (byte*)(_htable + _config.HashTableSize);
            _stabs = (int*)bp;
            // encoding slab max size and pos like -> 32 : 0
            _stabs[0] = 0x200000; _stabs[1] = 0x400001;
            _slabs = (SlabList*)_align((byte*)(_stabs + 1 + (iLastSlabSize + SizeofRcp) / iQuantum));
            // calculate slabs indexes and max sizes.
            int sbts = 64, scnt = 2, pos = scnt;
            for (var half = sbts / 2; sbts < iLastSlabSize; half = sbts / 2)
            {
                for (var es = sbts + half; sbts < es; sbts += iQuantum) _stabs[pos++] = scnt | (es << 16);
                scnt++;
                for (var es = sbts + half; sbts < es; sbts += iQuantum) _stabs[pos++] = scnt | (es << 16);
                scnt++;
            }
            _stabs[pos] = scnt;
            bp = _align((byte*)(_slabs + scnt + 1));
            // pre-allocate ~32k into every slab slot.
            var quot = sbts / 4;
            while (scnt-- > 0)
            {
                RefCountPtr* ls = null;
                var count = 0;
                for (var todo = 32768; todo > 0; count++, todo -= sbts, bp += sbts)
                {
                    var rc = (RefCountPtr*)bp;
                    rc->_list = ls; ls = rc;
                }
                _slabs[scnt].Set(ls, count);
                sbts -= quot;
                if ((scnt & 1) != 0) quot = sbts / 4;
            }
            // pre-allocate 256k into hash table buckets;
            _hb_cp = (HashBucket*)bp;
            _hb_ep = (HashBucket*)(bp += 256 * 1024);
            // pre-allocate ~1mb into big-mem chunks.
            _sbigs = null; _cntBigs = 0; sbts = iBigBlockSize + SizeofRcp;
            for (int left = 1024 * (1024 + 16); left > 0; _cntBigs++, left -= sbts, bp += sbts)
            {
                var rc = (RefCountPtr*)bp;
                rc->_list = _sbigs; _sbigs = rc;
            }
            // pre-allocate large memory pages for cache purposes.
            if (!_config.ReserveMemory)
                _cntPages = 1 + (int)((Stats.MemoryLimit - 1) / _config.AllocPageSize);
            if (_lpList != null)
                if (_config.ReserveMemory) { }
                else
                {
                    for (var ls = _lpList; ls != null; ls = ls.next)
                        ls.Release();
                    _lpList = null;
                }
            else if (_config.ReserveMemory)
                for (var bytes = Stats.MemoryLimit; bytes > 0; bytes -= _lpList.size)
                    _lpList = new LargePage(_virtual_memory, _config.AllocPageSize) { next = _lpList };
            _lpNext = _lpList;
            _cp = bp; _ep = _lpBase.data + _lpBase.size;
        }
    }
}

