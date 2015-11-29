using System;
using System.IO;
using System.Text;
using System.Threading;

namespace Dataflow.Serialization
{
    /// <summary>
    /// SpinLock class abstraction for PCL compa.
    /// </summary>
    public struct SpinWaitLock
    {
        private readonly object _locker;

        public SpinWaitLock(object locker) 
        {
            _locker = locker;
        }

        public void Enter(ref bool lockTaken)
        {
            Monitor.Enter(_locker, ref lockTaken);
        }

        public void Exit()
        {
            Monitor.Exit(_locker);
        }
    }

    //-- Thread local buffers for quick text merge manipulations/etc.

    public class TlsBufs
    {
        public const int ByteSize = 8192, CharSize = 8192;
        private char[] _cb;
        private byte[] _bt;
        [ThreadStatic]
        private static TlsBufs _tls;
        public static byte[] Bytes
        {
            get
            {
                var rc = _tls;
                if (rc != null) return rc.GetBytes();
                _tls = rc = new TlsBufs();
                return rc.GetBytes();
            }
            set
            {
                if (value.Length != ByteSize) return;
                var rc = _tls;
                if (rc != null) rc._bt = value;
            }
        }
        public static char[] Chars
        {
            get
            {
                var rc = _tls;
                if (rc != null) return rc.GetChars();
                _tls = rc = new TlsBufs();
                return rc.GetChars();
            }
            set
            {
                if (value.Length != CharSize) return;
                var rc = _tls;
                if (rc != null) rc._cb = value;
            }
        }
        public byte[] GetBytes()
        {
            var bt = _bt;
            if (bt != null) { _bt = null; return bt; }
            return _bt = new byte[ByteSize];
        }
        public char[] GetChars()
        {
            var cb = _cb;
            if (cb != null) { _cb = null; return cb; }
            return _cb = new char[CharSize];
        }
    }

    //-- StringBuilder replacement optimized for script sources formatting.

    public class TextBuilder : IDisposable
    {
        private char[] _cb;
        private int _pos, _indent;
        private StringBuilder _flush;

        public TextBuilder()
        {
            _cb = TlsBufs.Chars;
            _pos = 0;
        }

        public TextBuilder(StringBuilder sb)
        {
            _cb = TlsBufs.Chars;
            _flush = sb;
            _pos = 0;
        }

        public void Dispose()
        {
            _flush = null;
            if (_cb != null) TlsBufs.Chars = _cb;
        }

        public int Flush()
        {
            if (_flush == null) _flush = new StringBuilder(_cb.Length * 2);
            _flush.Append(_cb, 0, _pos);
            return _pos = 0;
        }

        public TextBuilder Append(char ch)
        {
            if (_pos < _cb.Length) _cb[_pos++] = ch;
            else { Flush(); _cb[_pos++] = ch; }
            return this;
        }

        public TextBuilder Append(string s)
        {
            if (s == null) return this;
            var i = _pos;
            foreach (var ch in s)
                if (i < _cb.Length) _cb[i++] = ch;
                else { _pos = i; i = Flush(); _cb[i++] = ch; }
            _pos = i;
            return this;
        }

        public TextBuilder Append(string s, int bp, int ep)
        {
            if (ep <= bp) return this;
            if (ep > s.Length) ep = s.Length;
            var i = _pos;
            while (bp < ep)
                if (i < _cb.Length) _cb[i++] = s[bp++];
                else { _pos = i; i = Flush(); _cb[i++] = s[bp++]; }
            _pos = i;
            return this;
        }

        public TextBuilder Append(string s0, string s1)
        {
            return Append(s0).Append(s1);
        }

        public TextBuilder Append(string s0, string s1, string s2)
        {
            return Append(s0).Append(s1).Append(s2);
        }

        public TextBuilder Append(string s0, string s1, string s2, string s3)
        {
            return Append(s0).Append(s1).Append(s2).Append(s3);
        }

        public TextBuilder Append(params string[] ps)
        {
            foreach (var s in ps) Append(s);
            return this;
        }

        public TextBuilder Append(int i)
        {
            if (i == 0) return Append('0');
            if (i < 0) { Append('-'); i = -i; }
            var sz = 5;
            if (i < 10000) sz = i < 100 ? (i < 10 ? 1 : 2) : (i < 1000 ? 3 : 4);
            else for (var x = i / 100000; x > 0; x = x / 10) sz++;
            var ip = _pos + sz;
            if (ip > _cb.Length) ip = Flush() + sz;
            _pos = ip;
            for (var dx = i; i != 0; i = dx)
                _cb[--ip] = (char)(i - (dx /= 10) * 10 + '0');
            return this;
        }

        public TextBuilder Base64(int i)
        {
            var cp = _pos;
            if (cp + 6 >= _cb.Length) cp = Flush();
            var bt64 = Text.Base64Encode;
            for (var vt = (uint)i; vt > 0; vt = vt >> 6)
                _cb[cp++] = (char)bt64[(int)(vt & 63)];
            _pos = cp;
            return this;
        }

        public TextBuilder Hex(int si)
        {
            uint i = (uint)si;
            var cp = _pos;
            var sz = 8;
            if (i < 0x10000)
                sz = i < 0x100 ? 2 : 4;
            if (cp + 2 + sz >= _cb.Length) cp = Flush();
            var bt16 = Text.Base16Bytes;
            _cb[cp] = '0'; _cb[cp + 1] = 'x';
            _pos = (cp += sz + 2);
            for( ; sz-- > 0; i = i >> 4)
                _cb[--cp] = (char)bt16[i & 15];
            return this;
        }

        public TextBuilder NewLine(string s) { return NewLine().Append(s); }
        public TextBuilder NewLine()
        {
            if (_pos > 0) { Append('\r'); Append('\n'); }
            for (var i = _indent; i > 0; i--) Append('\t');
            return this;
        }
        public TextBuilder NewLine(string s, string s1) { return NewLine(s).Append(s1); }
        public TextBuilder NewLine(string s, string s1, string s2) { return NewLine(s).Append(s1).Append(s2); }
        public TextBuilder NewLine(params string[] ps) { return NewLine().Append(ps); }

        public TextBuilder Indent() { _indent++; return this; }
        public TextBuilder Indent(string s) { NewLine(s); _indent++; return this; }
        public TextBuilder IndentAt(string s) { Append(s); _indent++; return this; }
        public TextBuilder UnIndent() { _indent--; return this; }
        public TextBuilder UnIndent(string s) { _indent--; return NewLine(s); }
        public TextBuilder OpenScope() { NewLine().Append('{'); _indent++; return this; }
        public TextBuilder CloseScope() { _indent--; return NewLine().Append('}'); }

        public TextBuilder Separator(char ch, ref bool insert)
        {
            if (insert) Append(ch);
            else insert = true;
            return this;
        }
        public TextBuilder Pop()
        {
            if (_pos > 0) _pos--;
            else throw new InvalidOperationException();
            return this;
        }
        public TextBuilder Comma() { return Append(','); }
        public TextBuilder Semic() { return Append(';'); }
        public TextBuilder Quote { get { return Append('"'); } }
        public TextBuilder Space() { return Append(' '); }
        public TextBuilder Nop() { return this; }

        public string ToString2()
        {
            var tb = _cb; _cb = null;
            TlsBufs.Chars = tb;
            if (_flush == null)
                return new string(tb, 0, _pos);
            if (_pos > 0)
                _flush.Append(tb, 0, _pos);
            return _flush.ToString();
        }
    }

    //-- Fast bitmask recorders.

    public struct BitSet64
    {
        private ulong _bits;
        private readonly int _offset;
        public int Offset { get { return _offset; } }

        public BitSet64(params int[] ps)
        {
            _bits = 0;
            int min = int.MaxValue, max = 0;
            foreach (var i in ps)
            {
                if (min > i) min = i;
                if (max < i) max = i;
            }
            if (max - min > 63) throw new ArgumentOutOfRangeException();
            _offset = min;
            foreach (var i in ps) Set(i);
        }
        public bool Has(int i) { i -= _offset; return i >= 0 && (_bits & 1UL << i) != 0; }
        public void Set(int i) { _bits |= 1UL << (i - _offset); }
        public bool this[int i] { get { return Has(i); } }
    }

    public struct BitSet128
    {
        private ulong _bits, _bit2;
        private readonly int _offset;

        public BitSet128(params int[] ps)
        {
            _bits = _bit2 = 0;
            int min = int.MaxValue, max = 0;
            foreach (var i in ps)
            {
                if (min > i) min = i;
                if (max < i) max = i;
            }
            if (max - min > 127) throw new ArgumentOutOfRangeException();
            _offset = min;
            foreach (var i in ps) Set(i);
        }
        public bool Has(int i)
        {
            if (i < _offset) return false;
            return (i -= _offset) < 64 ? (_bits & 1UL << i) != 0 : (_bit2 & 1UL << (i - 64)) != 0;
        }
        public void Set(int i)
        {
            if (i < _offset) return;
            if ((i -= _offset) < 64) _bits |= 1UL << i;
            else _bit2 |= 1UL << (i - 64);
        }
        public bool this[int i] { get { return Has(i); } }
    }

    //-- Bytes reader and writer using DataStorage and other memory models.

    public class StorageReader 
    {
        private byte[] _db;
        private int _bp, _cp, _epos;
        protected int _limit, _bsize;
        protected DataStorage _dts;

        //protected int LdSize { get { return _epos - _cp; } }
        public int Limit { get { return _limit; } set { _limit = value; } }
        public int Position { get { return _cp - _bp; } }

        public StorageReader(byte[] bt, int offset, int count)
        {
            _db = bt;
            _cp = offset;
            _bsize = _epos = _cp + count;
        }
        
        public StorageReader(DataStorage ds)
        {
            if(ds != null) Reset(ds);
        }

        public void Reset(DataStorage ds)
        {
            _dts = ds;
            _epos = 32;
            _cp = 0;
            _db = _dts.Peek(ref _epos, ref _cp);
            _bsize = _epos;
            _epos += (_bp = _cp);
        }

        internal byte[] Debug(int pos, int size)
        {
            var bt = new byte[size];
            Buffer.BlockCopy(_db, _cp + pos, bt, 0, size);
            return bt;
        }

        public bool IsAvailable(int bytes)
        {
            var sz = _epos - _cp;
            if (sz >= bytes) return true;
            if (_dts != null)
                return _dts.ContentSize - (_cp - _bp) >= bytes;
            throw new DataflowException("not enough data");
        }

        // return continuos block of data, creating new byte[] if necessary.
        public byte[] GetBytes(byte[] bt, ref int btSize)
        {
            int sz = _epos - _cp, lz = btSize;
            btSize = 0;
            if (lz <= _bsize)
            {
                if (lz > sz) GetData(lz, lz);
                if (bt == null) { btSize = _cp; bt = _db; }
                else Buffer.BlockCopy(_db, _cp, bt, 0, lz);
                _cp += lz; _limit -= lz;
                return bt;
            }
            if (bt == null) bt = new byte[lz];
            Buffer.BlockCopy(_db, _cp, bt, 0, sz);
            var pos = sz;
            for (_cp += sz, lz -= sz; lz > 0; _cp += sz)
            {
                var np = GetData(lz, 1);
                sz = _epos - np;
                if (sz > lz) sz = lz;
                Buffer.BlockCopy(_db, _cp, bt, pos, sz);
                lz -= sz; pos += sz;
            }
            _limit -= bt.Length;
            return bt;
        }

        // reads char from UTF8 bytes.
        public char GetChar()
        {
            var i = _cp;
            while (i < _epos)
            {
                var c = (int)_db[i];
                if (c > 0x7F)
                    if (c < 0xE0)
                    {
                        if (i + 1 >= _epos) break;
                        c = ((c & 0x1F) << 6) | (_db[++i] & 0x3F);
                    }
                    else
                    {
                        if (i + 2 >= _epos) break;
                        c = ((c & 0x0F) << 12) | ((_db[++i] & 0x3F) << 6);
                        c = c | ((_db[++i] & 0x3F) << 6);
                    }
                _cp = i + 1;
                return (char)c;
            }
            if (LoadData(1) == 0)
                return (char)0;
            return GetChar();
        }

        public int GetByte()
        {
            if (_epos - _cp < 1) _cp = GetData(1, 1);
            int bt = _db[_cp];
            _limit -= 1; _cp += 1;
            return bt;
        }

        public int GetB32()
        {
            if (_epos - _cp < 4) _cp = GetData(4, 4);
            int ui = BitConverter.ToInt32(_db, _cp);
            _limit -= 4; _cp += 4;
            return ui;
        }

        public uint GetB32BE()
        {
            if (_epos - _cp < 4) _cp = GetData(4, 4);
            var ui = (uint)_db[_cp + 3] + ((uint)_db[_cp + 2] << 8) + ((uint)_db[_cp + 1] << 16) + ((uint)_db[_cp] << 24);
            _limit -= 4; _cp += 4;
            return ui;
        }

        public long GetB64()
        {
            if (_epos - _cp < 8) _cp = GetData(8, 8);
            var ul = BitConverter.ToInt64(_db, _cp);
            _limit -= 8; _cp += 8;
            return ul;
        }

        public ulong GetB64BE()
        {
            ulong ul = GetB32BE() << 32;
            return ul + GetB32BE();
        }

        public int GetData(int sz, int rqs)
        {
            var lz = LoadData(sz);
            if (lz < rqs)
                DataflowException.Throw("cannot read enough data");
            return _cp;
        }

        public void UndoIntPB(int i)
        {
            var sz = Pbs.i32(i);
            _limit += sz; _cp -= sz;
        }

        public int GetIntPB()
        {
            var i = _cp;
            if (_epos - _cp < 10) i = GetData(5, 1);
            uint b0 = _db[i];
            if (b0 < 0x80)
            {
                _limit--; _cp = i + 1;
                return (int)b0;
            }
            do
            {
                uint b1 = _db[++i]; b0 = (b0 & 0x7F) | (b1 << 7);
                if (b1 < 0x80) break;
                b0 = (b0 & 0x3FFF) | ((b1 = _db[++i]) << 14);
                if (b1 < 0x80) break;
                b0 = (b0 & 0x1FFFFF) | ((b1 = _db[++i]) << 21);
                if (b1 < 0x80) break;
                b0 = (b0 & 0xFFFFFFF) | ((b1 = _db[++i]) << 28);
                if (b1 >= 0x80)
                {
                    for (int n = 0; n < 4; n++)
                        if (_db[++i] != 0xFF) DataflowException.Throw("bad varint");
                    if (_db[++i] != 0x01) DataflowException.Throw("bad varint");
                }
            } while (false);
            _limit -= (++i - _cp); _cp = i;
            return (int)b0;
        }

        public long GetLongPB()
        {
            var i = _cp;
            if (_epos - i < 10) i = GetData(10, 1);
            int b0 = _db[i++];
            if (b0 < 0x80) { _limit--; _cp = i; return b0; }
            b0 = (b0 & 0x7F) | (_db[i++] << 7);
            if (b0 < 0x4000) { _limit -= 2; _cp = i; return b0; }
            var tval = (long)Pbs.VarInt64Ex(_db, ref i, (uint)b0);
            _limit -= i - _cp; _cp = i;
            return tval;
        }

        internal int LoadData(int sz)
        {
            var lz = _epos - _cp;
            if (lz >= sz) return lz;
            // when stream hits eof we null it.
            if (_dts != null)
            {
                if (_dts.Skip(_cp - _bp) > lz)
                {
                    lz = sz < 32 ? 32 : (sz > 8192 ? 8192 : sz);
                    _bp = 0;
                    _db = _dts.Peek(ref lz, ref _bp);
                    _epos = (_cp = _bp) + lz;
                }
                else _bp = _cp;
            }
            _bsize = _epos - _cp;
            return lz;
        }

        // single byte look-ahead.
        public int PeekByte()
        {
            if (_epos <= _cp)
                if (LoadData(1) == 0) return 0;
            return _db[_cp];
        }
        
        // moves current position in the data stream forward.
        public void Skip(int i)
        {
            _limit -= i;
            var sz = _epos - _cp;
            // fast skip inside the buffer.
            if (i <= sz) { _cp += i; return; }
            if (_dts != null)
            {
                _dts.Skip(i + _cp - _bp);
                _cp = _epos = 0;
            }
            else _cp = _epos = 0;
        }

        // "returns" unused bytes back to storage.
        public void SyncToStorage()
        {
            var sz = _cp - _bp;
            if (sz == 0 || _dts == null) 
                return;
            _bp = _cp;
            _dts.Skip(sz);
        }
    }

    //-- Base class for message writers that produce byte streams. 

    public class StorageWriter
    {
        private byte[] _db;
        private int _cp;
        private int _bp, _epos;
        private DataStorage dts;

        public StorageWriter(DataStorage ds, int estimate = 0)
        {
            dts = ds;
            _cp = _bp = _epos = 0;
            _db = null;
            Reset(ds, estimate);
        }

        public StorageWriter(byte[] bts, int pos, int count)
        {
            _db = bts;
            _bp = _cp = pos;
            _epos = _cp + count;
            if (_epos > _db.Length) throw new ArgumentException();
            dts = null;
        }

        public StorageWriter(int reserveSize) : this(new byte[reserveSize], 0, reserveSize) { }

        // will try to flush writer buffered data into the storage.
        private int FlushEx(int endPos, int extraSz) 
        {
            if (dts == null)
            {
                if (endPos < _epos) return endPos;
                throw new SerializationException("data buffer full");
            }
            SetSegment(dts.Commit(endPos - _bp, extraSz));
            return _cp;
        }

        // make sure we have free space for the next writes.
        private int Flush(int xp, int sz)
        {
            return xp + sz <= _epos ? xp : FlushEx(xp, sz);
        }

        // flush unwritten data to storage implementations.
        public void Flush()
        {
            if (_cp == _bp || dts == null)
                return;
            dts.Commit(_cp - _bp, 0);
            _bp = _cp;
        }

        public void Reset(DataStorage ds, int estimate = 0)
        {
            if ((dts = ds) != null)
                SetSegment(dts.GetNextSegment(estimate));
        }

        private void SetSegment(DataSegment ds)
        {
            _bp = ds.Offset;
            _epos = _bp + ds.Size;
            _cp = _bp += ds.Count;
            _db = ds.Buffer;
        }

        public void WriteByte(byte c)
        {
            int i = _cp;
            if (i < _epos)
                _db[i] = c;
            else
            {
                i = Flush(i, 1);
                _db[i] = c;
            }
            _cp = i + 1;
        }

        public void Write(byte[] bt) { WriteBytes(bt, 0, bt.Length); }

        public void WriteByte(byte b1, byte b2)
        {
            int i = _cp;
            if (i + 2 > _epos) i = Flush(i, 2);
            _db[i + 0] = b1;
            _db[i + 1] = b2;
            _cp = i + 2;
        }

        public void WriteUTF8Char(char c)
        {
            var i = _cp;
            // ascii codes shortcut.
            if (c < 0x80)
            {
                if (i >= _epos) i = Flush(i, 1);
                _db[i] = (byte)c;
                _cp = i + 1;
                return;
            }
            // utf8 expanded codes path.
            if (i + 3 > _epos) i = Flush(i, 3);
            var bt = _db;
            if (c < 0x800)
                bt[i] = (byte)(0xC0 | (c >> 6));
            else
            {
                bt[i] = (byte)(0xE0 | ((c >> 12) & 0x0F));
                bt[++i] = (byte)(0x80 | ((c >> 6) & 0x3F));
            }
            bt[++i] = (byte)(0x80 | (c & 0x3F));
            _cp = i + 1;
        }

        public void WriteB32(uint v)
        {
            int i = _cp;
            if (i + 4 > _epos) i = Flush(i, 4);
            _db[i] = (byte)v; _db[i + 1] = (byte)(v >> 8);
            _db[i + 2] = (byte)(v >> 16); _db[i + 3] = (byte)(v >> 24);
            _cp = i + 4;
        }

        public void WriteB32BE(uint v)
        {
            int i = _cp;
            if (i + 4 > _epos) i = Flush(i, 4);
            _db[i + 3] = (byte)v; _db[i + 2] = (byte)(v >> 8);
            _db[i + 1] = (byte)(v >> 16); _db[i] = (byte)(v >> 24);
            _cp = i + 4;
        }

        public void WriteB64(ulong v)
        {
            var i = _cp;
            if (i + 8 > _epos) i = Flush(i, 8);
            _db[i] = (byte)v; _db[i + 1] = (byte)(v >> 8);
            _db[i + 2] = (byte)(v >> 16); _db[i + 3] = (byte)(v >> 24);
            _db[i + 4] = (byte)(v >> 32); _db[i + 5] = (byte)(v >> 40);
            _db[i + 6] = (byte)(v >> 48); _db[i + 7] = (byte)(v >> 56);
            _cp = i + 8;
        }

        public void WriteB64BE(ulong v)
        {
            var i = _cp;
            if (i + 8 > _epos) i = Flush(i, 8);
            _db[i + 7] = (byte)v; _db[i + 6] = (byte)(v >> 8);
            _db[i + 5] = (byte)(v >> 16); _db[i + 4] = (byte)(v >> 24);
            _db[i + 3] = (byte)(v >> 32); _db[i + 2] = (byte)(v >> 40);
            _db[i + 1] = (byte)(v >> 48); _db[i + 0] = (byte)(v >> 56);
            _cp = i + 8;
        }

        public void WriteBytes(int i, byte[] bt)
        {
            var pos = 0;
            do
            {
                int sz = _epos - _cp, lz = i - pos;
                if (sz >= lz)
                {
                    Buffer.BlockCopy(bt, pos, _db, _cp, lz);
                    _cp += lz;
                    return;
                }
                if (sz > 0)
                {
                    Buffer.BlockCopy(bt, pos, _db, _cp, sz);
                    pos += sz;
                }
                _cp = Flush(_epos, 1);
            } while (true);
        }

        public void WriteBytes(byte[] bs, int bp, int cnt)
        {
            var bt = _db;
            var i = _cp;
            if (cnt <= _epos - i)
            {
                _cp += cnt;
                if (cnt < 12)
                    while (cnt-- > 0) bt[i++] = bs[bp++];
                else Buffer.BlockCopy(bs, bp, bt, i, cnt);
            }
            else
            {
                while (cnt-- > 0)
                {
                    if (i >= _epos) { i = Flush(i, 1); bt = _db; }
                    bt[i++] = bs[bp++];
                }
                _cp = i;
            }
        }

        public void WriteIntPB(int i1, int i2)
        {
            WriteIntPB(i1);
            WriteIntPB(i2);
        }

        public void WriteIntPB(int v)
        {
            if (v >= 0)
            {
                var i = _cp;
                if (i + 5 > _epos) i = Flush(i, 5);
                var x = (uint)v;
                do
                {
                    if (x < 0x80) 
                    { 
                        _db[i++] = (byte)x; 
                        _cp = i; 
                        return; 
                    }
                    _db[i++] = (byte)(x | 0x80);
                    x = x >> 7;
                } while (true);
            }
            WriteLongPB(v);
        }

        public void WriteLongPB(long v)
        {
            var i = _cp;
            if (i + 10 > _epos) i = Flush(i, 10);
            var x = (ulong)v;
            do
            {
                if (x < 0x80)
                {
                    _db[i++] = (byte)x; 
                    _cp = i; 
                    break;
                }
                _db[i++] = (byte)(x | 0x80);
                x = x >> 7;
            } while (true);
        }

        public void WriteIntAL(int i)
        {
            if (i == 0)
            {
                WriteByte((byte)'0');
                return;
            }
            if (i < 0)
            {
                WriteByte((byte)'-');
                i = -i;
            }
            var sz = 5;
            if (i < 10000) sz = i < 100 ? (i < 10 ? 1 : 2) : (i < 1000 ? 3 : 4);
            else for (var x = i / 100000; x > 0; x = x / 10) sz++;
            var ip = _cp + sz;
            if (ip > _epos) ip = Flush(_cp, sz) + sz;
            _cp = ip;
            for (var dx = i; i != 0; i = dx)
            {
                var bt = (byte)(i - (dx /= 10) * 10 + '0');
                _db[--ip] = bt;
            }
        }

        public void WriteIntAL8(int i)
        {
            var ip = _cp + 8;
            if (ip > _epos) ip = Flush(_cp, 8) + 8;
            for (var dx = i; i != 0; i = dx)
            {
                var bt = (byte)(i - (dx /= 10) * 10 + '0');
                _db[--ip] = bt;
            }
            while (--ip >= _cp) _db[ip] = (byte)'0';
            _cp += 8;
        }

        public void WriteLongAL(long i, bool quote_longs = true)
        {
            // for JSON encoding anything above 2^52 must be string encoded to preserve precision on roundtrip.
            const int num_bits = 52, billion = 1000000000;
            var sign = false;
            if (i < 0)
            {
                sign = true;
                i = -i;
            }

            if ((i >> 31) != 0)
            {
                if (sign) WriteByte((byte)'-');
                WriteIntAL((int)i);
            }
            else
            {
                if (quote_longs && (i >> num_bits) != 0)
                    WriteByte((byte)'"');
                if (sign) WriteByte((byte)'-');

                long i2 = i / billion;
                i -= i2 * billion;
                if (i2 < billion)
                    WriteIntAL((int)i2);
                else
                {
                    long i3 = i2 / billion;
                    WriteIntAL((int)i3);
                    i2 -= i3 * billion;
                    WriteIntAL8((int)i2);
                }
                WriteIntAL8((int)i);
                if (quote_longs && (i >> num_bits) != 0)
                    WriteByte((byte)'"');
            }
        }

        public void WriteString(string s)
        {
            int i = _cp - 1, ep = _epos - 3;
            var bt = _db;
            foreach (var c in s)
            {
                if (++i >= ep) { i = Flush(i, 3); bt = _db; }
                if (c < 0x80) bt[i] = (byte)c;
                else
                {
                    if (c < 0x800)
                        bt[i] = (byte)(0xC0 | (c >> 6));
                    else
                    {
                        bt[i] = (byte)(0xE0 | ((c >> 12) & 0x0F));
                        bt[++i] = (byte)(0x80 | ((c >> 6) & 0x3F));
                    }
                    bt[++i] = (byte)(0x80 | (c & 0x3F));
                }
            }
            _cp = i + 1;
        }

        public void WriteStringUE(string s)
        {
            WriteStringA(s); //TODO: put real URL encoding logic here.
        }

        public void WriteStringA(string s)
        {
            var i = _cp - 1;
            var bt = _db;
            foreach (var c in s)
                if (++i < _epos)
                    bt[i] = (byte)c;
                else
                {
                    i = Flush(i, 1); bt = _db;
                    bt[i] = (byte)c;
                }
            _cp = i + 1;
        }

        // writes byte array segment in Base64 encoding.
        public void WriteBase64String(byte[] dt, int pos, int size)
        {
            byte[] bs64 = Text.Base64Encode, bt = _db;
            int i = _cp, left = size % 3, ep = pos + size - left;
            for (var rp = pos; rp < ep; rp += 3)
            {
                if (i + 4 > _epos) { i = Flush(i, 4); bt = _db; }
                var b1 = (dt[rp] << 16) | (dt[rp + 1] << 8) | dt[rp + 2];
                bt[i + 0] = bs64[b1 >> 18];
                bt[i + 1] = bs64[(b1 >> 12) & 63];
                bt[i + 2] = bs64[(b1 >> 6) & 63];
                bt[i + 3] = bs64[b1 & 63];
                i += 4;
            }
            if (left > 0)
            {
                if (i + 4 > _epos) { i = Flush(i, 4); bt = _db; }
                bt[i + 3] = (byte)'=';
                int b1 = dt[ep];
                bt[i] = bs64[b1 >> 2];
                b1 = (b1 & 3) << 4;
                if (left == 1)
                {
                    bt[i + 1] = bs64[b1];
                    bt[i + 2] = (byte)'=';
                }
                else
                {
                    int b2 = dt[ep + 1];
                    bt[i + 1] = bs64[(b2 >> 4) | b1];
                    bt[i + 2] = bs64[(b2 & 15) << 2];
                }
                i += 4;
            }
            _cp = i;
        }

        // returns writer contents as byte array.
        public byte[] ToByteArray()
        {
            var sz = _cp - _bp;
            if (sz == 0) return null;
            if (dts != null && dts.ContentSize > 0)
            {
                Flush();
                var bts = dts.ToByteArray();
                dts.Reset();
                SetSegment(dts.GetNextSegment(1));
                return bts;
            }
            if (sz == _db.Length) return _db;
            var bt = new byte[sz];
            Buffer.BlockCopy(_db, _bp, bt, 0, sz);
            return bt;
        }

        public void ToStream(Stream stream)
        {
            var sz = _cp - _bp;
            if (sz == 0) return;
            if (dts != null && dts.ContentSize > 0)
            {
                Flush();
                dts.ToStream(stream);
            }
            else stream.Write(_db, 0, sz);
        }

        public override string ToString()
        {
            var sz = _cp - _bp;
            if (sz == 0) return null;
            if (dts != null && dts.ContentSize > 0)
            {
                Flush();
                return dts.ToString();
            }
            return Encoding.UTF8.GetString(_db, 0, sz);
        }
    }

    //-- Caching data store based on byte[] segments.
    public class DataStorage : IDisposable
    {
        private DataSegment _list, _last;
        private int _cp;

        public string ContentType { get; set; }
        public int ContentSize { get; private set; }
        public int TransmittedCount { get; private set; }

        public bool IsEmpty { get { return ContentSize == 0; } }

        public DataStorage() { }
        public DataStorage(int size)
        {
            _list = _last = size <= LOHeapSegmentCache.KB08.BlockSize ? LOHeapSegmentCache.KB08.Get() : new DataSegment(new byte[size], 0, size);
        }

        public void Dispose()
        {
            if (_last == null) 
                return;
            _last = _list = _list.Release(true);
        }

        public static void Dispose(ref DataStorage ds)
        {
            if (ds == null) return;
            var ts = ds; ds = null;
            ts.Dispose();
        }

        public void Append(byte[] data)
        {
            var sz = data.Length;
            if (sz == 0) return;
            ContentSize += sz;
            // insert big data as own segment.
            if (sz > LOHeapSegmentCache.KB32.BlockSize)
            {
                var ns = new DataSegment(data, 0, sz) { Count = sz };
                if (_last != null) _last.Next = ns;
                else _list = ns;
                _last = ns;
                return;
            }
            // add initial block if needed.
            if (_last == null)
                _list = _last = LOHeapSegmentCache.KB32.Get();
            var bz = _last.FreeSpace;
            if (bz > sz) { bz = sz; sz = 0; }
            else sz -= bz;
            Buffer.BlockCopy(data, 0, _last.Buffer, _last.Offset + _last.Count, bz);
            _last.Count += bz;
            if (sz <= 0) return;
            // copy remainder of the data to new segment.
            _last = (_last.Next = LOHeapSegmentCache.KB32.Get());
            Buffer.BlockCopy(data, bz, _last.Buffer, _last.Offset, sz);
            _last.Count = sz;
        }

        public DataSegment GetNextSegment(int bytes)
        {
            if (_last != null && _last.FreeSpace > bytes)
                return _last;
            var ds = (ContentSize < 0x4000 && bytes < 0x8000 ? LOHeapSegmentCache.KB08 : LOHeapSegmentCache.KB32).Get();
            return _last = (_last == null ? _list = ds : _last.Next = ds);
        }

        public DataSegment GetSegmentToRead(int bytes = 0) 
        { 
            return GetNextSegment(bytes); 
        }

        public void ReadCommit(int size)
        {
            ContentSize += size;
            TransmittedCount += size;
            _last.Count += size;
        }

        public DataSegment GetSegmentToSend()
        {
            if (ContentSize == 0) return null;
            if (_list == null || _list.Count == 0) throw new InvalidOperationException();
            return _list;
        }

        public bool CommitSentBytes()
        {
            var ds = _list;
            TransmittedCount += ds.Count;
            ContentSize -= ds.Count;
            ds.Count = 0;
            if (ds == _last) return true;
            _list = ds.Next;
            ds.Release(false);
            return ContentSize == 0;
        }

        public DataSegment Commit(int count, int extend)
        {
            if (count == 0) 
                return _last;
            ContentSize += count;
            count += _last.Count;
            if (count > _last.Size) throw new ArgumentException();
            _last.Count = count;
            return extend <= 0 ? _last : GetNextSegment(extend);
        }

        public DataStorage Reset()
        {
            if (ContentSize == 0)
                return this;
            if (_list != _last)
            {
                _list.Next.Release(true);
                _list.Next = null;
                _last = _list;
            }
            _last.Count = _cp = 0;
            ContentSize = TransmittedCount = 0;
            return this;
        }

        // gets storage contents as single byte[].
        public byte[] ToByteArray()
        {
            var bts = new byte[ContentSize];
            if (_last == _list)
            {
                if (_last != null && _last.Count != 0)
                    Buffer.BlockCopy(_last.Buffer, _last.Offset + _cp, bts, 0, bts.Length);
                return bts;
            }
            // slow path, when data does not fit into single buffer.
            int pos = _list.Count - _cp;
            Buffer.BlockCopy(_list.Buffer, _list.Offset + _cp, bts, 0, pos);
            for (var ls = _list.Next; ls != null; ls = ls.Next)
            {
                Buffer.BlockCopy(ls.Buffer, ls.Offset, bts, pos, ls.Count);
                pos += ls.Count;
            }
            return bts;
        }

        public void ToStream(Stream ds)
        {
            if (_list != null) _list.ToStream(ds, _cp);
        }

        // gets storage contents as UTF8 encoded string.
        public override string ToString()
        {
            if (_last == _list)
                return (_last == null || _last.Count == 0) ? string.Empty : Encoding.UTF8.GetString(_last.Buffer, _last.Offset + _cp, _last.Count);
            // slow path, when data does not fit into single buffer.
            return Encoding.UTF8.GetString(ToByteArray(), 0, ContentSize);
        }

        // removes data region from the storage.
        public void Cut(int size, int offset)
        {
            // update content size.
            if (offset + size <= ContentSize)
                ContentSize -= size;
            else ContentSize = offset;
            offset += _cp;
            // check if data block is affected.
            var ds = _list;
            do
            {
                var cnt = ds.Count;
                if (cnt <= offset)
                    offset -= cnt;
                else
                {
                    size = ds.Cut(offset, size);
                    if (size == 0) return;
                    for (var ns = ds.Next; ns != null; )
                        if (ns.Count > size)
                        {
                            ns.Cut(0, size);
                            return;
                        }
                        else
                        {
                            ns.Release(false);
                            ds.Next = ns = ns.Next;
                        }
                    _last = ds;
                }
            } while ((ds = ds.Next) != null);
        }
        
        // returns continuos data buffer for the region of data inside storage.
        public byte[] Peek(ref int peekSize, ref int peekOffset)
        {
            if (_list == null) { peekSize = 0; return null; }
            // we are trying to avoid data copy into temp buffer.
            var pos = _cp + peekOffset;
            var bx = _list;
            do
            {
                int dc = bx.Count;
                if (pos >= dc) pos -= dc;
                else
                {
                    dc -= pos;
                    pos += bx.Offset;
                    if (dc >= peekSize || bx.Next == null)
                    {
                        peekSize = dc; peekOffset = pos;
                        return bx.Buffer;
                    }
                    var nx = bx.Next;
                    //note: may add checking more 'next' buffers in future.
                    peekSize = Math.Min(peekSize, dc + nx.Count);
                    var bt = new byte[peekSize];
                    Buffer.BlockCopy(bx.Buffer, pos, bt, 0, dc);
                    Buffer.BlockCopy(nx.Buffer, nx.Offset, bt, dc, peekSize - dc);
                    peekOffset = 0;
                    return bt;
                }
            } while ((bx = bx.Next) != null);
            // peeking past avail data.
            peekSize = 0;
            return null;
        }
        
        // moves current position in the data stream forward.
        public int Skip(int skip)
        {
            ContentSize -= skip;
            // if skip past eof clear storage.
            if (ContentSize <= 0)
            {
                Reset();
                return 0;
            }
            var sz = _list.Count - _cp;
            // if skip inside current first block, inc cp.
            if (skip < sz)
                _cp += skip;
            else
            {
                skip -= sz;
                var next = _list.Next;
                do
                {
                    // if list head is not init block, release it.
                    if (_list != _last)
                        _list.Release(false);
                    if (next == null)
                        throw new DataflowException("storage state");
                    _list = next;
                    // if skip lands inside new list head update cp and we are done.
                    if (skip < next.Count)
                    {
                        _cp = skip;
                        break;
                    }
                    // going to remove next/list block on loop repeat.
                    skip -= next.Count;
                    next = next.Next;
                } while (true);
            }
            return ContentSize;
        }
    }

    public class DataSegment
    {
        // internal maintenance pointers.
        internal DataSegment Next;
        internal LOHeapSegment LHS;

        public byte[] Buffer { get; private set; }
        public int Count { get; set; }
        public int Offset { get; private set; }
        public int Size { get; private set; }
        public int FreeSpace { get { return Size - Count; } }

        public DataSegment(byte[] buffer, int offset, int size)
        {
            Buffer = buffer;
            Offset = offset;
            Size = size;
        }

        internal DataSegment(LOHeapSegment bk, int ps, int sz)
        {
            LHS = bk;
            Buffer = bk.Buffer;
            Offset = ps;
            Size = sz;
        }

        internal DataSegment Release(bool chain)
        {
            if (chain && Next != null)
                Next.Release(chain);
            Next = null;
            if (LHS != null)
                LHS.Host.Release(this);
            else
            {
                // non-LOH segment based buffer.
                Buffer = null;
                Count = 0;
            }
            return null;
        }

        public int Cut(int pos, int sz)
        {
            int xz = pos + sz;
            if (xz >= Count)
                if (pos >= Count) return sz;
                else
                {
                    xz -= Count;
                    Count = pos;
                    return xz;
                }
            System.Buffer.BlockCopy(Buffer, Offset + xz, Buffer, Offset + pos, Count - xz);
            Count -= sz;
            return 0;
        }

        public void ToStream(Stream ms, int cpos)
        {
            if (Count > cpos)
            {
                ms.Write(Buffer, Offset + cpos, Count - cpos);
                cpos = 0;
            }
            else cpos -= Count;

            if (Next != null)
                Next.ToStream(ms, 0);
        }

        public override string ToString()
        {
            return string.Format("seg:{1}[{0}]", Size, Offset / Size);
        }
    }

    //-- Provides slices of byte array memory that belongs to LOH.
    internal class LOHeapSegment
    {
        internal LOHeapSegmentCache Host { get; private set; }
        internal byte[] Buffer { get; private set; }
        internal int Issued { get; set; }

        internal LOHeapSegment(LOHeapSegmentCache hs, int sz)
        {
            Host = hs;
            Buffer = new byte[sz];
        }

        internal DataSegment GetDataSegments(int sz, out int count)
        {
            var allocated = 0;
            DataSegment ls = null;
            for (var pos = 0; pos + sz <= Buffer.Length; pos += sz)
            {
                var ns = new DataSegment(this, pos, sz) { Next = ls };
                allocated++;
                ls = ns;
            }
            count = allocated;
            return ls;
        }

        internal void Release()
        {
            if (Buffer == null)
                return;
            if (Issued != 0)
                throw new InvalidOperationException();

            // release buffer and invalidate other fields.
            Issued = -(int.MaxValue / 2);
            Buffer = null;
            Host = null;
        }
    }

    //-- Cache for LOH byte[] based data segments.
    public class LOHeapSegmentCache
    {
        private SpinWaitLock _lock;
        private DataSegment _freeList;
        private int _freeCount;
        private int _keepCount;

        public int HeapCount { get; private set; }
        public int BlockSize { get; private set; }

        public static LOHeapSegmentCache KB08 = new LOHeapSegmentCache(0x2000, 64);
        public static LOHeapSegmentCache KB32 = new LOHeapSegmentCache(0x8000, 16);

        public LOHeapSegmentCache(int dsegSize, int keepCount = 64)
        {
            _lock = new SpinWaitLock(this);
            BlockSize = dsegSize;
            _keepCount = keepCount;
        }

        // Gets cached LOH byte array segment.
        public DataSegment Get()
        {
            DataSegment ds = null;
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                if ((ds = _freeList) == null)
                {
                    if (_freeCount != 0)
                        throw new InvalidOperationException("corrupt LOH segment cache");
                    // allocating new memory segments in 256kb increments.
                    var nhs = new LOHeapSegment(this, 0x40000);
                    ds = nhs.GetDataSegments(BlockSize, out _freeCount);
                    HeapCount++;
                }
                // take data segment from free list.
                _freeList = ds.Next;
                ds.Next = null;
                _freeCount--;
                // count issued data segments from LOH segment.
                ds.LHS.Issued++;
            }
            finally
            {
                if(lockTaken) _lock.Exit();
            }
            return ds;
        }

        // Returns data segment to the cache.
        public void Release(DataSegment ds)
        {
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);

                ds.Count = 0;
                ds.Next = _freeList;
                _freeList = ds;
                ds.LHS.Issued--;
                _freeCount++;

                if (_freeCount > _keepCount)
                    PurgeCache(_keepCount);
            }
            finally
            {
                if(lockTaken) _lock.Exit();
            }
        }

        // Tries to release complete unused heap segments.
        public void Purge(int keep)
        {
            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);
                PurgeCache(0);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
        }

        protected void PurgeCache(int keep)
        {
            if (_freeCount <= keep)
                return;
            var redoCount = 0;
            DataSegment ns = null, redoList = null;
            for (var ls = _freeList; ls != null; ls = ns)
            {
                ns = ls.Next;
                if (ls.LHS.Issued > 0)
                {
                    ls.Next = redoList;
                    redoList = ls;
                    redoCount++;
                    continue;
                }
                // release will check state and do it once internally.
                ls.LHS.Release();
            }
            _freeCount = redoCount;
            _freeList = redoList;
        }
    }
}
