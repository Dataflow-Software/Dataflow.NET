/* 
 * Key-Value memcache for .NET, using virtual(non-GC) memory.
 * Supports in-process api and binary memcached protocols over tcp.
 * Optimized for multi-get/pipelined access profile.
 * (c) Viktor Poteryakhin, 2013-2015.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Dataflow.Caching
{
    public unsafe partial class LocalCache
    {
        #region static utility/support methods.

        protected static void _memcopy(byte* ds, byte* bs, int cnt)
        {
            var ls = (long*)bs;
            for (; cnt >= 8; ds += 8, cnt -= 8) *(long*)ds = *ls++;
            for (bs = (byte*)ls; cnt > 0; cnt--) *ds++ = *bs++;
        }

        protected static void _memcopy(byte* ds, byte[] bt, int pos, int cnt)
        {
            while (--cnt >= 0) *ds++ = bt[pos++];
        }

        protected static void _memcopy_rc(byte* dst, RefCountPtr* src, int cnt)
        {
            var ss = (byte*)(src + 1);
            if (cnt <= iBigBlockSize) _memcopy(dst, ss, cnt);
            else
            {
                _memcopy(dst, ss, iBigBlockSize);
                _memcopy_rc(dst + iBigBlockSize, src->Next, cnt - iBigBlockSize);
            }
        }

        protected static void _memcopy(byte[] dst, RefCountPtr* src, int cnt)
        {
            if (cnt < 32)
            {
                var ss = (byte*)(src + 1);
                for (int i = 0; i < cnt; i++) dst[i] = *ss++;
                return;
            }
            fixed (byte* ds = dst)
                _memcopy_rc(ds, src, cnt);
        }

        protected static void _dbkcopy(RefCountPtr* rc, byte* src, int cnt)
        {
            var ds = (byte*)(rc + 1);
            if (cnt <= iBigBlockSize) _memcopy(ds, src, cnt);
            else
            {
                _memcopy(ds, src, iBigBlockSize);
                _dbkcopy(rc->Next, src + iBigBlockSize, cnt - iBigBlockSize);
            }
        }

        protected static int _align8(int cnt) { return (cnt + 7) & ~0x7; }
        protected static uint _get_BE(byte* bp) { return ((uint)bp[0] << 24) + ((uint)bp[1] << 16) + ((uint)bp[2] << 8) + (uint)bp[3]; }

        protected static void _copy_with_key(DataKey* dst, DataKey* src)
        {
            // copy value fields.
            dst->flags = src->flags;
            dst->etime = src->etime;
            dst->cas = src->cas;
            dst->val_addr = null;
            dst->status = 0;
            dst->lrup = dst->lrux = null;
            dst->next = null;
            // copy key part.
            dst->hash = src->hash;
            var ds = (byte*)(dst + 1);
            var ls = (long*)(src + 1);
            var cnt = src->KeySize;
            dst->size = (uint)cnt;
            for (; cnt >= 8; ls++, ds += 8, cnt -= 8) *(long*)ds = *ls;
            for (var bs = (byte*)ls; cnt > 0; bs++, ds++, cnt--) *ds = *bs;
        }

        protected static void _init_dk(DataKey* dk, int opc)
        {
            ulong* bp = (ulong*)dk, ep = (ulong*)(dk + 1);
            while (bp < ep) *bp++ = 0;
            dk->opcode = (byte)opc;
        }

        protected static void _set_key(DataKey* ce, byte* key)
        {
            const ulong principal = 1099511628211;
            var ds = (byte*)(ce + 1);
            ulong hv = 14695981039346656037;
            for (var cnt = ce->KeySize; cnt > 0; key++, cnt--)
            {
                var cb = *key;
                *ds++ = cb;
                hv = (hv * principal) ^ (ulong)cb;
            }
            ce->hash = hv;
        }

        protected static void _set_key(DataKey* ce, byte[] key, int cnt)
        {
            const ulong principal = 1099511628211;
            var ds = (byte*)(ce + 1);
            ulong hv = 14695981039346656037;
            ce->size = (uint)cnt;
            for (var i = 0; i < cnt; i++)
            {
                var cb = key[i];
                *ds++ = cb;
                hv = (hv * principal) ^ (ulong)cb;
            }
            ce->hash = hv;
        }

        protected static byte* _skip(RefCountPtr* bp, int skip) { return (byte*)(bp + 1) + skip; }
        protected static string _get_key(DataKey* ce) { return new string((sbyte*)(ce + 1), 0, ce->KeySize, System.Text.Encoding.UTF8); }
        
        protected static uint _swap(uint i)
        {
            uint x = i >> 24;
            x |= (i >> 8) & 0xFF00;
            x |= (i << 8) & 0xFF0000;
            return x | (i << 24);
        }

        protected static ulong _swap(ulong i)
        {
            ulong u = _swap((uint)i);
            u = (u << 32) | _swap((uint)(i >> 32));
            return u;
        }

        #endregion

        // layout sensitive struct defs.
        [StructLayout(LayoutKind.Explicit)]
        public unsafe struct DataKey
        {
            // shared/cache use fields.
            [FieldOffset(0)]
            public DataKey* next;    // next entry in bucket list.
            [FieldOffset(8)]
            public ulong hash;       // key hash value in Ketama or FNV algo.
            [FieldOffset(16)]
            public ulong cas;       // memcached concurrent access control.
            [FieldOffset(24)]
            public uint flags;      // flags set by user.
            [FieldOffset(28)]
            public uint etime;      // expiration time.
            [FieldOffset(32)]
            public uint size;       // 0-7: key size, 8-31: data size
            [FieldOffset(40)]
            public RefCountPtr* val_addr;
            //[FieldOffset( 40 )] public ulong val_data;  // contains data value when data size = 8
            [FieldOffset(48)]
            public DataKey* lrup;   // previous item in LRU list.
            [FieldOffset(56)]
            public DataKey* lrux;   // next item in LRU list.
            [FieldOffset(64)]
            public uint flushMark;  // on create - flush generation mark.
            [FieldOffset(68)]
            public uint options;    // keeps special status etc
            // network request only (overlap) fields.
            [FieldOffset(48)]
            public uint opaque;     // tag value sent from client.
            [FieldOffset(52)]
            public byte status;
            [FieldOffset(54)]
            public byte opcode;
            [FieldOffset(56)]
            public long def_val;   // default value for inc/dec opcodes.
            [FieldOffset(64)]
            public long longval;

            public int ValSize { get { return (int)(size >> iKeySizeBits); } set { size = ((uint)value << iKeySizeBits) + (size & iKeySizeMask); } }
            public int KeySize { get { return (int)(size & iKeySizeMask); } }

            public bool CheckCas(DataKey* rqs)
            {
                var rcs = rqs->cas;
                if (rcs != 0 && cas != rcs) return true;
                rqs->cas = cas = rcs + 1;
                return false;
            }

            public void SetValue(DataKey* src)
            {
                size = src->size;
                var rc = src->val_addr;
                val_addr = rc;
                if (rc != null) rc->AddRef();
            }

            public bool GetNumeric(ref long number)
            {
                int sz = ValSize;
                if (val_addr == null || sz > 22) return false;
                var bp = _skip(val_addr, 0);
                var ep = bp + sz;
                long val = 0;
                for (uint pos = 1; ep-- != bp; pos *= 10)
                {
                    uint x = (uint)*ep - '0';
                    if (x > 9) return false;
                    val += pos * x;
                }
                number = val;
                return true;
            }

            public void SetNumeric(long i)
            {
                var bp = _skip(val_addr, 0);
                var ep = bp + 22;
                do
                {
                    long x = i / 10;
                    *(--ep) = (byte)(i - x * 10 + '0');
                    i = x;
                } while (i != 0);
                int count = 22 - (int)(ep - bp);
                _memcopy(bp, ep, ValSize = count);
            }

            public static bool Equal(DataKey* ce, DataKey* cx)
            {
                if (cx->hash != ce->hash) return false;
                var cnt = ce->KeySize;
                if (cnt != cx->KeySize) return false;
                var ks = (byte*)(ce + 1);
                var lp = (long*)(cx + 1);
                for (; cnt >= 8; lp++, ks += 8, cnt -= 8)
                    if (*lp != *(long*)ks) return false;
                for (var bs = (byte*)lp; cnt > 0; bs++, ks++, cnt--)
                    if (*bs != *ks) return false;
                return true;
            }

            public void AppendValue(byte* src, int cnt, int skip)
            {
                var sz = ValSize;
                if (cnt + skip > sz) throw new ArgumentException("append:val");
                if (sz < iBigBlockSize)
                    _memcopy(_skip(val_addr, skip), src, cnt);
                else
                {
                    var rc = val_addr;
                    for (; skip >= iBigBlockSize; skip -= iBigBlockSize) rc = rc->Next;
                    var cp = _skip(rc, skip);
                    sz = iBigBlockSize - skip;
                    if (sz >= cnt) _memcopy(cp, src, cnt);
                    else
                    {
                        _memcopy(cp, src, sz);
                        _dbkcopy(rc->Next, src + sz, cnt - sz);
                    }
                }
            }

            public void SetExtras(byte* bp, int count)
            {
                // TODO: check if GAT and TOUCH may affect simple logic assumptions
                switch (count)
                {
                    case 8:
                        flags = *(uint*)bp; // store in direct BE format.
                        etime = _get_BE(bp + 4);
                        break;
                    case 4:
                        flags = *(uint*)bp;
                        break;
                    case 20:
                        flags = *(uint*)(bp + 16);
                        long v1 = _get_BE(bp), v2 = _get_BE(bp + 4);
                        longval = v2 + (v1 << 32);
                        v1 = _get_BE(bp + 8); v2 = _get_BE(bp + 12);
                        def_val = v2 + (v1 << 32);
                        break;
                    default: throw new NotSupportedException();
                }
            }
        }

        [StructLayout(LayoutKind.Explicit)]
        public struct HashBucket
        {
            [FieldOffset(0)]
            private ulong _data;
            // bucket data access methods.
            public int Push(DataKey* ne)
            {
                var i = _data;
                var cnt = ++i & 0x7FFF;
                ne->next = (DataKey*)(i >> 16);
                _data = ((ulong)ne << 16) | cnt;
                return (int)cnt;
            }
            public void Split(HashBucket* ht, int shift, int mask)
            {
                for (var i = 0; i <= mask; i++) ht[i] = new HashBucket();
                for (DataKey* nx, ls = List; ls != null; ls = nx)
                {
                    nx = ls->next;
                    var ix = (int)(ls->hash >> shift) & mask;
                    ht[ix].Push(ls);
                }
                Htab = ht;
            }
            public void Pop(DataKey* prev)
            {
                var i = _data;
                if (prev == null)
                {
                    var ne = (DataKey*)(i >> 16);
                    _data = ((ulong)ne->next << 16) | (--i & 0x7FFF);
                }
                else
                {
                    var ne = prev->next;
                    prev->next = ne->next;
                    _data--;
                }
            }
            public bool IsList { get { return (_data & 0x8000) == 0; } }
            public DataKey* List { get { return (DataKey*)(_data >> 16); } }
            public HashBucket* Htab { get { return (HashBucket*)(_data >> 16); } set { _data = ((ulong)value << 16) | 0x8000; } }
            public int Count { get { return (int)(_data & 0x7FFF); } }
        }

        [StructLayout(LayoutKind.Explicit)]
        public struct Packet
        {
            // network packet bytes layout.
            [FieldOffset(0)]
            public byte magic;
            [FieldOffset(1)]
            public byte opcode;
            [FieldOffset(2)]
            private ushort _keylen;
            [FieldOffset(4)]
            public byte extlen;
            [FieldOffset(5)]
            public byte datatype;
            [FieldOffset(6)]
            public ushort reserv;
            [FieldOffset(7)]
            public byte status;
            [FieldOffset(8)]
            private uint _total;
            [FieldOffset(12)]
            public uint opaque;
            [FieldOffset(16)]
            public ulong _cas;
            // fields data access methods.
            public ulong Cas
            {
                get
                {
                    var i = _cas;
                    if (i == 0) return i;
                    var u = (uint)i;
                    ulong v = (u >> 24) | ((u >> 8) & 0xFF00) | ((u << 8) & 0xFF0000) | (u << 24);
                    u = (uint)(i >> 32);
                    u = (u >> 24) | ((u >> 8) & 0xFF00) | ((u << 8) & 0xFF0000) | (u << 24);
                    return (v << 32) | (ulong)u;
                }
                set
                {
                    var i = value;
                    if (i < 256) { _cas = i << 56; return; }
                    var u = (uint)i;
                    ulong v = (u >> 24) | ((u >> 8) & 0xFF00) | ((u << 8) & 0xFF0000) | (u << 24);
                    u = (uint)(i >> 32);
                    u = (u >> 24) | ((u >> 8) & 0xFF00) | ((u << 8) & 0xFF0000) | (u << 24);
                    _cas = (v << 32) | (ulong)u;
                }
            }
            // note: key len is limited to 256 in current memcached.
            public uint Keylen { get { uint i = _keylen; return ((i >> 8) | (i << 8)) & iKeySizeMask; } set { _keylen = (ushort)(((value << 8) | (value >> 8)) & iKeySizeMask); } }
            // note: total limit is 1mb+ and easily fits into 3 bytes.
            public uint Total
            {
                get
                {
                    uint i = _total, v = (i >> 24);
                    v |= (i << 8) & 0xFF0000;
                    v |= (i >> 8) & 0x00FF00;
                    return v;
                }
                set
                {
                    var i = value;
                    if (i < 256) _total = i << 24;
                    else _total = (i << 24) | ((i >> 8) & 0xFF00) | ((i << 8) & 0xFF0000);
                }
            }
        }
    }
}
