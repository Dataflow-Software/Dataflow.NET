﻿using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;

namespace Dataflow.Serialization
{
    // Protocol Buffers message field kinds, as defined in the Google standard specification.
    public enum FieldKind
    {
        Unknown = 0, Required = 1, Optional = 2, Repeated = 3, Enum = 4, Map = 5
    }
    
    // Lists all built-in types supported for message fields. Includes extensions to the Google specification, like Currency, Date and Decimal.
    public enum DataType
    {
        Int32, Int64, UInt32, UInt64, SInt32, SInt64,
        Bool, Float, Double, Fixed32, Fixed64,
        SFixed32, SFixed64, Bytes, String, Enum,
        Message, Date, Decimal, Object, Currency, MapEntry, Undefined, LastIndex = Undefined
    }

    // lists all supported .NET types for native DataReader/Writer operations.
    // if any existing values are changed, all client proto files should be recompiled.
    public enum WireType
    {
        None = 0, Int32 = 1, Int64 = 2, Sint32 = 3, Sint64 = 4,
        String = 5, Bytes = 6, Date = 7, Decimal = 8,
        Bit32 = 9, Bit64 = 10, Float = 11, Double = 12,
        Bool = 13, Char = 14, Message = 15, Enum = 16,
        Currency = 17, MapEntry = 18, MaxValue = 20
    }

    // list of stream data formats implemented by streaming RPC channels.
    [Flags]
    public enum DataEncoding : uint
    {
        Unknown = 0, Proto = 1, Json = 2, QueryString = 4, Xml = 8, Memcached = 16, Any = 0x1F
    }

    // In-place, mutable, generic-typed list, implements repeated fields in messages.
    // Similar to .NET generic List<T> in functionality, but implemented as struct to avoid extra allocations on message creation.
    public struct Repeated<T>
    {
        private static T[] _empty = new T[0];
        private T[] _items;
        private int _count;

        public int Count { get { return _count; } }

        private T[] FitToSize()
        {
            Array.Resize(ref _items, _count);
            return _items;
        }

        public T[] Items
        {
            get { return _count > 0 ? (_count == _items.Length ? _items : FitToSize()) : _empty; }
            set { _count = (_items = value) != null ? value.Length : 0; }
        }
        
        //public Array AsArray { get { return _items??_empty; } }
        public Repeated(T[] s) { _items = s; _count = s !=null ? s.Length : 0; }
        public bool IsEmpty { get { return _count == 0; } }
        public T FirstOrDefault { get { return Count == 0 ? default(T) : _items[0]; } }

        // implements assignment into T[] destinations.
        public static implicit operator T[](Repeated<T> s) { return s.Items; }
        // implements assignments from T[] sources.
        public static implicit operator Repeated<T>(T[] s) { return new Repeated<T>(s); }
        // gets or sets the element at the specified index.
        public T this[int i]
        {
            get { return _items[i]; }
            set { _items[i] = value; }
        }
        
        public T Add(T ne)
        {
            var cnt = _count;
            if (cnt == 0) _items = new T[4];
            else if (cnt == _items.Length)
                Array.Resize(ref _items, cnt * 2);
            _count++;
            return _items[cnt] = ne;
        }
        
        public void Add(T[] na) { AddRange(na, 0, na.Length); }
        
        public void AddRange(T[] na, int pos, int count)
        {
            if (na == null || count == 0) 
                return;
            if (_items == null && na.Length == count)
            {
                _items = na;
                _count = na.Length;
            }
            else
            {
                int sz = _items.Length;
                if (_count + count > sz)
                    Array.Resize(ref _items, _count + count);
                Array.Copy(na, pos, _items, _count, count);
                _count += count;
            }
        }
        
        public void Clear() 
        {
            if (_count == 0) return;
            _items = null;
            _count = 0; 
        }

        public void Remove(T item) 
        {
            if (_items == null) return;
            var pos = Array.IndexOf(_items, item, 0, _count);
            if(pos < 0) 
                return;
            _count--;
            if(_count >= pos)
                _items[pos] = _items[_count];
        }

        // convert to standard enumerable formats.
        public T[] ToArray() { return Items; }
        public List<T> ToList()
        {
            var al = new List<T>(_count);
            for (var i = 0; i < _count; i++)
                al.Add(_items[i]);
            return al;
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    public struct ValueStorage
    {
        [FieldOffset(0)]
        public bool _bool;
        [FieldOffset(0)]
        public int _int32;
        [FieldOffset(0)]
        public long _int64;
        [FieldOffset(0)]
        public float _single;
        [FieldOffset(0)]
        public double _double;
        [FieldOffset(0)]
        public byte _byte;
    }

    // Exception classes for Dataflow libraries.
    public class DataflowException : Exception
    {
        public DataflowException(string s) : base(s) { }
        public static void Throw(string msg) { throw new DataflowException(msg); }
        public static void WriteOnce(string s) { throw new DataflowException("can't change the value " + s); }
    }

    public class SerializationException : DataflowException
    {
        public SerializationException(string s) : base(s) { }
        public static void NotImplemented(WireType wt)
        {
            throw new DataflowException("not supported: " + wt.ToString());
        }
    }

    // efficient long-int based type for currency values manipulations.
    public struct Currency
    {
        public const int Scale = 10000;
        public static Currency Zero = new Currency(0);
        private long _value;
        public Currency(long i) { _value = i; }
        public Currency(long u, int c) { _value = u * Scale + c; }
        // non-PCL method : public Currency(Decimal i) { _value = Decimal.ToOACurrency(i); }
        public Currency(double d) { _value = (long)(d * Scale); }
        public long Cents { get { return _value % Scale; } }
        public long Units { get { return _value / Scale; } set { _value = value * Scale; } }
        public long Value { get { return _value; } set { _value = value; } }

        public static Currency operator +(Currency c1, Currency c2) { return new Currency { _value = c1._value + c2._value }; }
        public static Currency operator -(Currency c1, Currency c2) { return new Currency { _value = c1._value - c2._value }; }
        public static Currency operator *(Currency c1, int i1) { return new Currency { _value = c1._value * i1 }; }
        public static implicit operator Currency(long i1) { return new Currency(i1); }
        public static explicit operator long(Currency c1) { return c1._value; }
        public static bool operator !=(Currency c1, Currency c2) { return c1._value != c2._value; }
        public static bool operator ==(Currency c1, Currency c2) { return c1._value == c2._value; }
        public static bool operator >(Currency c1, Currency c2) { return c1._value > c2._value; }
        public static bool operator <(Currency c1, Currency c2) { return c1._value < c2._value; }
        public static bool operator >=(Currency c1, Currency c2) { return c1._value >= c2._value; }
        public static bool operator <=(Currency c1, Currency c2) { return c1._value <= c2._value; }

        public Currency Abs() { return _value > 0 ? this : new Currency(-_value); }
        public Currency Max(Currency c) { return this > c ? this : c; }

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            return (obj is Currency) && this == (Currency)obj;
        }

        // non-PCL method : public decimal ToDecimal() { return Decimal.FromOACurrency(_value); }
        public double ToDouble() { return (double)_value / Scale; }
        public int ToInt() { return (int)(_value / Scale); }

        public bool TryParse(string s)
        {
            int uc = 0, i = 0, sign = 0;
            foreach (var ch in s)
            {
                if (ch < '0' || ch > '9')
                    if (i == 0)
                    {
                        if (ch == '-') sign = 1;
                        else if (ch == '+') sign = 2;
                        else return false;
                    }
                    else
                        if (ch == '.' && uc == 0) uc = i;
                        else return false;
                i++;
            }
            int cents = 0;
            if (uc != 0)
            {
                int k = uc;
                while (++k < i) cents = cents * 10 + (s[k] - '0');
                while (k - uc < 5) { cents *= 10; k++; }
                i = uc;
            }
            long units = 0;
            uc = sign == 0 ? 0 : 1;
            for (; uc < i; uc++)
                units = units * 10 + (s[uc] - '0');
            _value = units * Scale + (long)cents;
            if (sign == 1) _value = -_value;
            return true;
        }

        public override int GetHashCode() { return (int)_value; }
        public override string ToString()
        {
            var s = _value.ToString();
            var sz = s.Length;
            if (sz > 4)
                return s.Substring(0, sz - 4) + "." + s.Substring(sz - 4);
            while (sz < 4) { s = "0" + s; sz++; }
            return "0." + s;
        }
    }

    /// <summary>
    /// Static class for helper methods mostly related to Google Protocol Buffers encoding.
    /// </summary>
    public static partial class Pbs
    {
        public const int // PB specification wire data encoding codes
            iVarInt = 0, iBit64 = 1, iString = 2, iStartGroup = 3, iEndGroup = 4, iBit32 = 5, iBadCode = 8;
        public const int // FieldOpts const values must not be changed.
            iNone = 0, iBox = 2, iList = 1, iSpecial = 4, iKvMap = 8, iGoogleType = 16,
            szFixed32 = 4, szFixed64 = 8, szFloat = 4, szDouble = 8, szBool = 1;

        public static int i32(int val)
        {
            var i = (uint)val;
            if (i < 0x80) return 1;
            if (i < 0x4000) return 2;
            if (i < 0x200000) return 3;
            if (i < 0x10000000) return 4;
            return (int)i > 0 ? 5 : 10;
        }
        public static int i64(long val)
        {
            var i = (ulong)val;
            if ((i >> 31) == 0) return i32((int)val);
            if (i < 0x800000000) return 5;
            if (i < 0x40000000000) return 6;
            if (i < 0x2000000000000) return 7;
            if (i < ((ulong)1 << 56)) return 8;
            return i < ((ulong)1 << 63) ? 9 : 10;
        }
        public static int sign(int v) { return (v << 1) ^ (v >> 32); }
        public static long signl(long v) { return (v << 1) ^ (v >> 63); }
        public static int si32(int v) { return i32((v << 1) ^ (v >> 32)); }
        public static int si64(long v) { return i64((v << 1) ^ (v >> 63)); }
        public static int bln(bool v) { return 1; }
        public static int chr(char v) { return v < 128 ? 1 : 2; }
        public static int str(string s)
        {
            if (s == null) return 1;
            var i = GetUtf8ByteSize(s);
            return i + i32(i);
        }
        public static int szPfx(int sz) { return sz + Pbs.i32(sz); }
        public static int bts(byte[] b)
        {
            if (b == null) return 1;
            var i = b.Length;
            return i + i32(i);
        }
        public static int msg(Message msg)
        {
            if (msg == null) return 0;
            var i = msg.GetSerializedSize();
            return i + i32(i);
        }
        public static int cur(Currency cy) { return i64((long)cy); }
        public static int dat(DateTime dt) { return si64(DateToMsecs(dt)); }
        public static int dec(Decimal dt)
        {
            return dec2(Decimal.GetBits(dt));
        }
        public static int dec2(int[] di)
        {
            int si = di[3], sg = si < 0 ? 1 : 0;
            if (si == 0 && di[0] == 0 && di[1] == 0 && di[2] == 0) return 1;
            sg = sg | ((byte)(si >> 16) << 1);
            var sz = i32(di[0]);
            var li = (long)di[2]; li = li << 32;
            sz += i64(li | (uint)di[1]);
            sz += i32(sg);
            return sz + 1;
        }

        public static DateTime DtEpoch = new DateTime(1970, 1, 1);
        public static long DateToMsecs(DateTime dt)
        {
            return (dt.Ticks - DtEpoch.Ticks) / 10000;
        }
        public static DateTime DateFromMsecs(long ms)
        {
            return DtEpoch.AddMilliseconds(ms);
        }

        //- cause .NET does not care to provide any wrapper for memcmp().
        public static bool EqualBytes(byte[] a, byte[] b)
        {
            if (a == null)
                return b == null;
            if (b == null || b.Length != a.Length)
                return false;
            var i = 0;
            foreach (var ab in a)
                if (ab == b[i]) i++;
                else return false;
            return true;
        }
        public static uint SwapBytes(uint i)
        {
            return (i >> 24) | ((i & 0xFF0000) >> 8) | (i << 24) | ((i & 0xFF00) << 8);
        }

        public static int GetWireId(int id, WireType wt) { return (id << 3) | wt.WireFormat(); }
        public static int WireFormat(this WireType wt)
        {
            switch (wt)
            {
                case WireType.Int32:
                case WireType.Int64:
                case WireType.Sint32:
                case WireType.Sint64:
                case WireType.Bool:
                case WireType.Char:
                case WireType.Date:
                case WireType.Currency:
                case WireType.Enum:
                    return iVarInt;
                case WireType.String:
                case WireType.Bytes:
                case WireType.Decimal:
                case WireType.Message:
                case WireType.MapEntry:
                    return iString;
                case WireType.Bit32:
                case WireType.Float:
                    return iBit32;
                case WireType.Bit64:
                case WireType.Double:
                    return iBit64;
            }
            return iBadCode;
        }
        public static byte[] AsBytes(this string s) { return Encoding.UTF8.GetBytes(s); }
        
        // Gets number of unicode chars encoded into UTF8 byte array.
        public static int GetUtf8CharSize(byte[] bt)
        {
            int i = 0, k = 0;
            foreach (var b in bt)
            {
                if (k > 0) { k--; continue; }
                i++;
                if (b < 0x80) continue;
                k = b < 0xE0 ? 1 : 2;
            }
            return i;
        }
        // String size in UTF8 encoding, uses simplified checks
        public static int GetUtf8ByteSize(string s)
        {
            if (s.Length > 20)
                return Encoding.UTF8.GetByteCount(s);
            var i = 0;
            foreach (var c in s)
            {
                if (c < 0x80) i++;
                else if (c < 0x800) i += 2;
                else i += 3;
            }
            return i;
        }
        public static long GetDoubleBits(double dv) { return BitConverter.DoubleToInt64Bits(dv); }
        public static double SetDoubleBits(long lv) { return BitConverter.Int64BitsToDouble(lv); }
        public static int GetFloatBits(float dv)
        {
            var bt = BitConverter.GetBytes(dv);
            int lv;
            if (BitConverter.IsLittleEndian)
                lv = (bt[3] << 24) | (bt[2] << 16) | (bt[1] << 8) | bt[0];
            else lv = (bt[0] << 24) | (bt[1] << 16) | (bt[2] << 8) | bt[3];
            return lv;
        }
        public static float SetFloatBits(int iv) { return BitConverter.ToSingle(BitConverter.GetBytes(iv), 0); }

        // Returns text name for PB encoding markers.
        public static string GetWireTypeName(int id)
        {
            switch (id)
            {
                case iVarInt: return "varint";
                case iString: return "string";
                case iBit64: return "64-bit";
                case iStartGroup:
                case iEndGroup: return "group(deprecated)";
                case iBit32: return "32-bit";
                default: return "undefined";
            }
        }

        // Encode/decode helpers
        public static ulong VarInt64Ex(byte[] db, ref int pos, uint b0)
        {
            ulong ul = 0;
            uint i = (uint)pos, b = db[i];
            b0 = (b0 & 0x3FFF) | (b << 14);
            while (b > 0x7F)
            {
                b0 = (b0 & 0x1FFFFF) | ((b = db[++i]) << 21);
                if (b < 0x80) break;
                b0 &= 0xFFFFFFF;
                uint b1 = db[++i], b2 = 0;
                while (b1 > 0x7F)
                {
                    b1 = (b1 & 0x7F) | ((b = db[++i]) << 7);
                    if (b < 0x80) break;
                    b1 = (b1 & 0x3FFF) | ((b = db[++i]) << 14);
                    if (b < 0x80) break;
                    b1 = (b1 & 0x1FFFFF) | ((b = db[++i]) << 21);
                    if (b < 0x80) break;
                    b1 &= 0xFFFFFFF; b2 = (b = db[++i]);
                    if (b < 0x80) break;
                    b2 = (b2 & 0x7F) | ((b = db[++i]) << 7);
                    if (b > 127) DataflowException.Throw("varint value is too long");
                    break;
                }
                ul = ((ulong)b1 << 28) | ((ulong)b2 << 56);
                break;
            }
            pos = (int)++i;
            return ul | b0;
        }
        public static ulong VarInt64(byte[] db, ref int pos)
        {
            uint b0 = db[pos++];
            if (b0 < 0x80) return b0;
            b0 = (b0 & 0x7F) | (uint)db[pos++] << 7;
            return b0 < 0x4000 ? b0 : VarInt64Ex(db, ref pos, b0);
        }
        public static int PutString(byte[] db, string s, int i)
        {
            // buffer should have enough space.
            foreach (var c in s)
            {
                // fast UTF8 encoding rule, Java-style compliant.
                if (c < 0x80) db[i++] = (byte)c;
                else
                {
                    if (c < 0x800)
                        db[i++] = (byte)(0xC0 | (c >> 6));
                    else
                    {
                        db[i++] = (byte)(0xE0 | ((c >> 12) & 0x0F));
                        db[i++] = (byte)(0x80 | ((c >> 6) & 0x3F));
                    }
                    db[i++] = (byte)(0x80 | (c & 0x3F));
                }
            }
            return i;
        }

        public static int Align08(int i) { return (i + 7) & (~0x7); }
        public static int Align16(int i) { return (i + 15) & (~0xf); }
    }

    /// <summary>
    /// The root for all message classes generated by Dataflow Protocol Buffers Compiler for C#.
    /// Declares virtual methods that are implemented by compiler in produced classes.
    /// </summary>

    public class Message
    {
        public const string ClassName = "Message";
        public static readonly Message Empty = new Message();
        private static readonly MessageDescriptor _desc_ = new MessageDescriptor_20("message", 0, Empty);
        // cache for message size and bitmask
        protected int _memoized_size;
        // methods that are implemented by .proto compiler.
        // returns metadata object that helps runtime libraries in working with message content.
        public virtual MessageDescriptor GetDescriptor() { return _desc_; }
        // calculates message size in bytes in the Google Protocol Buffers format.
        public virtual int GetSerializedSize() { return _memoized_size; }
        // clears the message contents. All fields are marked as null, and set to the values specified in proto file or to the type defaults.
        public virtual void Clear() { _memoized_size = 0; }
        // compares two messages for equality, including sub-messages.
        //public virtual bool Equals(Message msg) { return false; }
        // reads value from data reader into the message field. 
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Get(FieldDescriptor fs, IDataReader dr) { }
        public virtual void Get(FieldDescriptor fs, TDataReader dr) { }
        // returns true if all required fields in the message and all embedded messages are set, false otherwise.
        public virtual bool IsInitialized() { return true; }
        // fast self-factory implementation.
        public virtual Message New() { return Empty; }
        // writes not null message fields to the data writer.
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Put(IDataWriter dw) { }
        // writes field value the data writer (or explicitly calls IsNull).
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void PutField(TDataWriter dw, FieldDescriptor fs) { }

        // public interface methods.

        [EditorBrowsable(EditorBrowsableState.Never)]
        public int ByteSize { get { var sz = _memoized_size; return (sz > 0) ? sz : GetSerializedSize(); } }
        
        public void MergeFrom(Message data)
        {
            new MessageCopier().Append(this, data);
        }
        
        public void MergeFrom(byte[] data)
        {
            if (data == null || data.Length == 0) return;
            new PBStreamReader(data, 0, data.Length).Read(this, data.Length);
        }

        public void MergeFrom(string data)
        {
            if (string.IsNullOrEmpty(data)) return;
            new JSStreamReader(data, 0, data.Length).Read(this);
        }

        public void WriteTo(System.IO.Stream os, DataEncoding encoding)
        {
            var dts = new DataStorage();
            switch (encoding)
            {
                case DataEncoding.Proto:
                    var pbw = new PBStreamWriter(dts);
                    pbw.Append(this);
                    pbw.Flush();
                    break;
                case DataEncoding.Json:
                    var jsw = new JSStreamWriter(dts);
                    jsw.Append(this);
                    jsw.Flush();
                    break;
                default: throw new ArgumentException();
            }
            dts.ToStream(os);
        }

        public byte[] ToByteArray()
        {
            var bytes = GetSerializedSize();
            var buffer = new byte[bytes];
            Put(new PBStreamWriter(buffer, 0, bytes));
            return buffer;
        }

        public byte[] ToByteArray(byte[] bt, int offset, int count)
        {
            Put(new PBStreamWriter(bt, offset, count));
            return bt;
        }

        public override string ToString()
        {
            return ToString(false);
        }

        public string ToString(bool decorate)
        {
            string jsonString = null;
            var dts = new DataStorage();
            try
            {
                var jsw = new JSStreamWriter(dts, decorate);
                jsw.Append(this);
                jsw.Flush();
                jsonString = dts.ToString();
            }
            finally
            {
                dts.Dispose();
            }
            return jsonString;
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public override bool Equals(object obj) { return base.Equals(obj); }
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() { return base.GetHashCode(); }

        // support for Get<FName> method in genrated maps.
        protected MapEntry GetMapEntry(MapEntry[] map, long key, MessageDescriptor ds)
        {
            foreach (var kv in map) if (kv.lk == key) return kv;
            if (ds != null) return new MapEntry(ds) { lk = key };
            throw new DataflowException("key not found in map: " + key);
        }

        protected MapEntry GetMapEntry(MapEntry[] map, string key, MessageDescriptor ds)
        {
            foreach (var kv in map) if (kv.sk == key) return kv;
            if (ds != null) return new MapEntry(ds) { sk = key };
            throw new DataflowException("key not found in map: " + key);
        }

        protected static MessageDescriptor _init_ds_(MessageDescriptor ds, Message factory, params FieldDescriptor[] fs)
        {
            return ds.Init(factory, fs);
        }

        protected static MessageDescriptor _map_ds_(int key, int val, MessageDescriptor vs = null)
        {
            return new MessageDescriptor_30("map", key, val, vs);
        }
    }

    // This is an alias to empty message definition.
    public sealed class Nothing : Message
    {
        public override MessageDescriptor GetDescriptor() { return Descriptor; }
        public static readonly MessageDescriptor Descriptor = new MessageDescriptor_20("Nothing", Pbs.iNone, new Nothing());
    }

    // Message descriptor and related metadata classes. 

    public class FieldDescriptor
    {
        public const int iRepeated = 64, iRequired = 128, iPacked = 256, iMap = 512, iSignFmt = 1024, iBoxed = 2048;
        private int _options;

        public int DataSize { get { return _options >> 16; } set { _options = (_options & 0xFFFF) | value << 16; } }
        public WireType DataType { get { return (WireType)(_options & 63); } }
        public bool IsBoxed { get { return (_options & iBoxed) != 0; } }
        public bool IsPacked { get { return (_options & iPacked) != 0; } }
        public bool IsRepeated { get { return (_options & iRepeated) != 0; } }
        public bool IsRequired { get { return (_options & iRequired) != 0; } }
        public bool IsSignedInt { get { return (_options & iSignFmt) != 0; } }

        public int Id { get; protected set; }
        public string Name { get; protected set; }
        public int Pos { get; internal set; }
        public MessageDescriptor MessageType { get; internal set; } // todo: protect again after JsonValue fixup

        public FieldDescriptor(string name, int id, int os) { Name = name; Id = id; _options = os; }
        public FieldDescriptor(string name, int id, int os, MessageDescriptor ds) { Name = name; Id = id; _options = os; MessageType = ds; }
        public FieldDescriptor(int pbid, int iwt, string name, MessageDescriptor ds = null) 
        { 
            Name = name;
            MessageType = ds;
            var wt = (WireType)iwt;
            if (wt == WireType.Sint32 || wt == WireType.Sint64)
                iwt |= iSignFmt;
            _options = iwt | iBoxed;
            Id = Pbs.GetWireId(pbid, wt); 
        }
    }

    public class EnumFieldDescriptor : FieldDescriptor
    {
        public EnumFieldDescriptor(string name, int value)
            : base(name, 0, 0)
        {
            Id = value;
        }
    }

    public class MessageDescriptor
    {
        private static FieldDescriptor[] _no_fields = new FieldDescriptor[0];
        protected FieldDescriptor[] _nameIndex;
        protected FieldDescriptor[] _idsIndex;
        protected FieldDescriptor[] _fields;
        protected Message _factory;
        protected int _options;

        public bool IsKvMap { get { return (_options & Pbs.iKvMap) != 0; } }
        public bool IsListType { get { return (_options & Pbs.iList) != 0; } }
        public bool IsBoxType { get { return (_options & Pbs.iBox) != 0; } }
        public bool IsGoogleType { get { return (_options & Pbs.iGoogleType) != 0; } }
        public bool IsSpecial { get { return (_options & Pbs.iSpecial) != 0; } }
        public bool HasOptions { get { return _options != 0; } }

        public FieldDescriptor[] Fields { get { return _fields; } }
        public int FieldCount { get { return _fields.Length; } }
        public Message Factory { get { return _factory; } }
        public string Name { get; protected set; }

        protected MessageDescriptor() { }
        protected MessageDescriptor(string name, int options, Message factory, params FieldDescriptor[] fs)
        {
            Name = name; 
            _options = options; 
            _factory = factory;
            if (fs == null)
            {
                _idsIndex = _fields = _no_fields; 
                return;
            }
            var pos = 0;
            _fields = fs;
            foreach (var fi in fs) fi.Pos = pos++;
            RecalcIndex();
        }

        public MessageDescriptor Init(Message factory, params FieldDescriptor[] fs)
        {
            if (_factory != null)
                throw new InvalidOperationException("init once");
            _factory = factory;
            if (fs == null)
                _idsIndex = _fields = _no_fields;
            else
            {
                var pos = 0;
                _fields = fs;
                foreach (var fi in fs) fi.Pos = pos++;
                RecalcIndex();
            }
            return this;
        }

        public FieldDescriptor AddField(string name, WireType type, MessageDescriptor desc, int options = 0)
        {
            var pos = _fields.Length;
            var fs = new FieldDescriptor(name, Pbs.GetWireId(pos + 1, type), (int)type | options, desc) { Pos = pos };
            Array.Resize(ref _fields, pos + 1);
            _idsIndex = _fields;
            return _fields[pos] = fs;
        }

        public Message New() { return _factory.New(); }
        
        public FieldDescriptor Find(int id)
        {
            id = id >> 3;
            // fast index available.
            if (_idsIndex != null)
            {
                return _idsIndex[id-1];
            }
            // PB indexes are sparse, full search required.
            var fcount = _fields.Length;
            if (fcount < 6)
            {
                foreach (var ds in _fields) 
                    if ((ds.Id >> 3) == id) 
                        return ds;
            }
            else
            {
                for (int r = 0, h = fcount - 1; r <= h; )
                {
                    var i = (r + h) >> 1;
                    var item = _fields[i];
                    var c = (item.Id >> 3) - id;
                    if (c < 0) r = i + 1;
                    else if (c == 0) return item; else h = i - 1;
                }
            }
            return null;
        }

        public FieldDescriptor Find(string name)
        {
            if (_nameIndex == null)
            {
                foreach (var fi in _fields)
                    if (fi.Name == name) return fi;
            }
            else
                for (int r = 0, h = _nameIndex.Length - 1; r <= h; )
                {
                    var i = (r + h) >> 1;
                    var item = _nameIndex[i];
                    var c = string.CompareOrdinal(item.Name, name);
                    if (c < 0) r = i + 1;
                    else if (c == 0) return item; else h = i - 1;
                }
            return null;
        }

        protected void RecalcIndex()
        {
            _nameIndex = _fields.Length < 8 ? null : _fields.OrderBy(fi => fi.Name).ToArray();
            if (_factory == null)
                return;
            var maxId = 0;
            foreach (var fi in _fields)
                if (fi.Id > maxId) maxId = fi.Id;
            maxId = maxId >> 3;
            if (maxId == FieldCount)
                _idsIndex = _fields;
            else if (maxId < FieldCount * 2)
            {
                _idsIndex = new FieldDescriptor[maxId];
                foreach (var fi in _fields)
                    _idsIndex[(fi.Id >> 3) - 1] = fi;
            }
        }
    }

    public class EnumDescriptor : MessageDescriptor
    {
        private readonly EnumFieldDescriptor[] _map;
        private readonly int _lowId, _maxId;
        protected EnumDescriptor() : base(null, 0, null) { }
        public EnumDescriptor(string name, params EnumFieldDescriptor[] fs) : base(name, 0, null)
        {
            Name = name;
            _fields = fs;
            if (fs == null) return;
            var pos = 0;
            _lowId = int.MaxValue; _maxId = int.MinValue;
            foreach (var ds in fs)
            {
                if (ds.Id > _maxId) _maxId = ds.Id;
                if (ds.Id < _lowId) _lowId = ds.Id;
                ds.Pos = pos++;
            }
            RecalcIndex();
            if (_maxId - _lowId >= FieldCount * 2) return;
            _map = new EnumFieldDescriptor[_maxId - _lowId + 1];
            foreach (var ds in fs) _map[ds.Id - _lowId] = ds;
        }
        public FieldDescriptor GetById(int id)
        {
            if (_map != null)
            {
                if (id < _lowId || id > _maxId) return null;
                return _map[id - _lowId];
            }
            foreach (var ds in _fields)
                if (ds.Id == id) return ds;
            return null;
        }
    }

    public class MessageDescriptor_20 : MessageDescriptor
    {
        public MessageDescriptor_20(string name, int options, Message factory, params FieldDescriptor[] fs) :
            base(name, options, factory, fs) { }
    }

    public class MessageDescriptor_30 : MessageDescriptor
    {
        private const string map_key_name = "key", map_value_name = "value";
        private static FieldDescriptor _key_string = new FieldDescriptor(1, (int)WireType.String, map_key_name);

        public MessageDescriptor_30(string name, int options = Pbs.iNone)
        {
            Name = name;
            _options = options;
        }

        public MessageDescriptor_30(string name, int key, int value, MessageDescriptor valMst = null) :
            base(name, Pbs.iKvMap, null, key == (int)WireType.String ? _key_string : new FieldDescriptor(1, key, map_key_name), new FieldDescriptor(2, value, map_value_name, valMst)) { }
    }

    // (de)Serialization interface definitions.

    public interface IDataReader
    {
        // value types deserializers.
        int AsBit32();
        long AsBit64();
        bool AsBool();
        byte[] AsBytes();
        char AsChar();
        Currency AsCurrency();
        DateTime AsDate();
        decimal AsDecimal();
        double AsDouble();
        int AsEnum(EnumDescriptor es);
        int AsInt();
        long AsLong();
        string AsString();
        float AsFloat();
        // special methods are needed due to Protocol Buffers signed int format optimizations.
        int AsSi32();
        long AsSi64();
        // message types deserializer, requires instance to read into.
        void AsMessage(Message msg, FieldDescriptor fs);
    }

    public abstract class TDataReader
    {
        // value types deserializers.
        public abstract int AsInt();
        public abstract long AsLong();
        public abstract string AsString();
        public abstract bool AsBool();
        public abstract byte[] AsBytes();
        public abstract char AsChar();
        public abstract Currency AsCurrency();
        public abstract DateTime AsDate();
        public abstract decimal AsDecimal();
        public abstract double AsDouble();
        public abstract int AsEnum(EnumDescriptor es);
        public virtual float AsFloat() { return (float)AsDouble(); }
        public virtual int AsBit32() { return AsInt(); }
        public virtual long AsBit64() { return AsLong(); }
        // special methods are needed due to Protocol Buffers signed int format optimizations.
        public virtual int AsSi32() { return AsInt(); }
        public virtual long AsSi64() { return AsLong(); }
        // message types deserializer, requires instance to read into.
        public abstract void AsMessage(Message msg, FieldDescriptor fs);
    }

    public abstract class TDataWriter
    {
        public abstract void IsNull(FieldDescriptor fs);
        // value types serializers.
        public abstract void AsString(FieldDescriptor fs, string s);
        public abstract void AsInt(FieldDescriptor fs, int i);
        public abstract void AsLong(FieldDescriptor fs, long l);
        public abstract void AsBytes(FieldDescriptor fs, byte[] bt);
        public abstract void AsDate(FieldDescriptor fs, DateTime dt);
        public abstract void AsDecimal(FieldDescriptor fs, decimal d);
        public abstract void AsDouble(FieldDescriptor fs, double d);
        public abstract void AsBool(FieldDescriptor fs, bool b);
        public abstract void AsChar(FieldDescriptor fs, char ch);
        public virtual void AsBit32(FieldDescriptor fs, int i) { AsInt(fs, i); }
        public virtual void AsBit64(FieldDescriptor fs, long l) { AsLong(fs, l); }
        public virtual void AsSi32(FieldDescriptor fs, int i) { AsInt(fs, i); }
        public virtual void AsSi64(FieldDescriptor fs, long l) { AsLong(fs, l); }
        public abstract void AsEnum(FieldDescriptor fs, int en);
        public abstract void AsFloat(FieldDescriptor fs, float f);
        public abstract void AsCurrency(FieldDescriptor fs, Currency cy);
        // repeated fields serializer, "expands" inside based on field data type.
        public abstract void AsRepeated(FieldDescriptor fs, Array data);
        // embedded messages serializer.
        public abstract void AsMessage(FieldDescriptor fs, Message msg);
    }

    public interface IDataWriter
    {
        // value types serializers.
        void AsString(FieldDescriptor fs, string s);
        void AsInt(FieldDescriptor fs, int i);
        void AsLong(FieldDescriptor fs, long l);
        void AsSi32(FieldDescriptor fs, int i);
        void AsSi64(FieldDescriptor fs, long l);
        void AsBytes(FieldDescriptor fs, byte[] bt);
        void AsDate(FieldDescriptor fs, DateTime dt);
        void AsDecimal(FieldDescriptor fs, decimal d);
        void AsDouble(FieldDescriptor fs, double d);
        void AsBool(FieldDescriptor fs, bool b);
        void AsChar(FieldDescriptor fs, char ch);
        void AsBit32(FieldDescriptor fs, int i);
        void AsBit64(FieldDescriptor fs, long l);
        void AsEnum(FieldDescriptor fs, int en);
        void AsFloat(FieldDescriptor fs, float f);
        void AsCurrency(FieldDescriptor fs, Currency cy);
        // repeated fields serializer, "expands" inside based on field data type.
        void AsRepeated(FieldDescriptor fs, Array data);
        // embedded messages serializer.
        void AsMessage(FieldDescriptor fs, Message msg);
    }

    // Base classes for PB/JSON/... (de)serializers.

    public abstract class MessageSerializer
    {
        protected readonly StorageWriter _storage;

        protected MessageSerializer(byte[] bts, int pos, int count)
        {
            _storage = new StorageWriter(bts, pos, count);
        }

        protected MessageSerializer(DataStorage dts, int estimate)
        {
            _storage = new StorageWriter(dts, estimate);
        }

        protected abstract void AppendMessage(Message msg, MessageDescriptor ci);

        public void Append(Message msg)
        {
            if (msg != null) 
                AppendMessage(msg, msg.GetDescriptor());
        }

        public void Flush()
        {
            _storage.Flush();
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public override bool Equals(object obj) { return base.Equals(obj); }
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() { return base.GetHashCode(); }
    }

    public abstract class MessageDeserializer
    {
        protected readonly StorageReader _storage;

        protected MessageDeserializer(StorageReader storage)
        {
            _storage = storage;
        }

        public abstract void Read(Message message, int size);

        [EditorBrowsable(EditorBrowsableState.Never)]
        public override bool Equals(object obj) { return base.Equals(obj); }
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() { return base.GetHashCode(); }
    }

    // implements Message.MergeFrom() logic.
    internal sealed class MessageCopier : IDataWriter
    {
        private readonly CopyReader _rdr;
        private Message _cur;
        private object _x;
        private long _v;

        public MessageCopier()
        {
            _rdr = new CopyReader { Data = this };
        }

        public void Append(Message dst, Message src)
        {
            _cur = dst;
            src.Put(this);
        }

        void IDataWriter.AsBit64(FieldDescriptor fs, long l) { _v = l; _cur.Get(fs, _rdr); }
        void IDataWriter.AsBool(FieldDescriptor fs, bool b) { _v = b ? 1 : 0; _cur.Get(fs, _rdr); }
        void IDataWriter.AsBytes(FieldDescriptor fs, byte[] bt) { _x = bt; _cur.Get(fs, _rdr); }
        void IDataWriter.AsDate(FieldDescriptor fs, DateTime dt) { _v = dt.Ticks; _cur.Get(fs, _rdr); }
        void IDataWriter.AsDecimal(FieldDescriptor fs, decimal d) { _x = d; _cur.Get(fs, _rdr); }
        void IDataWriter.AsDouble(FieldDescriptor fs, double d) { _v = (long)Pbs.GetDoubleBits(d); _cur.Get(fs, _rdr); }
        void IDataWriter.AsFloat(FieldDescriptor fs, float f) { _v = Pbs.GetFloatBits(f); _cur.Get(fs, _rdr); }
        void IDataWriter.AsInt(FieldDescriptor fs, int i) { _v = i; _cur.Get(fs, _rdr); }
        void IDataWriter.AsLong(FieldDescriptor fs, long l) { _v = l; _cur.Get(fs, _rdr); }
        void IDataWriter.AsString(FieldDescriptor fs, string s) { _x = s; _cur.Get(fs, _rdr); }
        void IDataWriter.AsCurrency(FieldDescriptor fs, Currency cy) { _v = cy.Value; _cur.Get(fs, _rdr); }
        void IDataWriter.AsChar(FieldDescriptor fs, char ch) { _v = ch; _cur.Get(fs, _rdr); }
        void IDataWriter.AsBit32(FieldDescriptor fs, int i) { _v = i; _cur.Get(fs, _rdr); }
        void IDataWriter.AsEnum(FieldDescriptor fs, int en) { _v = en; _cur.Get(fs, _rdr); }
        void IDataWriter.AsSi32(FieldDescriptor fs, int i) { _v = i; _cur.Get(fs, _rdr); }
        void IDataWriter.AsSi64(FieldDescriptor fs, long l) { _v = l; _cur.Get(fs, _rdr); }

        void IDataWriter.AsMessage(FieldDescriptor fs, Message msg)
        {
            var prev = _cur;
            _cur.Get(fs, _rdr);
            msg.Put(this);
            _cur = prev;
        }

        void IDataWriter.AsRepeated(FieldDescriptor fs, Array data)
        {
            throw new NotImplementedException();
        }

        internal sealed class CopyReader : IDataReader
        {
            public MessageCopier Data;
            public long AsBit64() { return Data._v; }
            public bool AsBool() { return Data._v != 0; }
            public byte[] AsBytes() { return Data._x as byte[]; }
            public DateTime AsDate() { return new DateTime(Data._v); }
            public decimal AsDecimal() { return (decimal)Data._x; }
            public double AsDouble() { return Pbs.SetDoubleBits(Data._v); }
            public float AsFloat() { return Pbs.SetFloatBits((int)Data._v); }
            public int AsInt() { return (int)Data._v; }
            public long AsLong() { return Data._v; }
            public void AsMessage(Message msg, FieldDescriptor fs) { Data._cur = msg; }
            public string AsString() { return Data._x as string; }
            public int AsBit32() { return (int)Data._v; }
            public char AsChar() { return (char)Data._v; }
            public Currency AsCurrency() { return new Currency(Data._v); }
            public int AsEnum(EnumDescriptor es) { return (int)Data._v; }
            public int AsSi32() { return (int)Data._v; }
            public long AsSi64() { return Data._v; }
        }
    }

    // converts Message content to object[] format.
    public sealed class ObjectWriter : IDataWriter
    {
        private object[] Data;

        public object[] Write(Message msg) 
        {
            Data = new object[msg.GetDescriptor().FieldCount];
            msg.Put(this);
            return Data;
        }

        void IDataWriter.AsBit32(FieldDescriptor fs, int iv) { Data[fs.Pos] = iv; }
        void IDataWriter.AsBit64(FieldDescriptor fs, long lv) { Data[fs.Pos] = lv; }
        void IDataWriter.AsBool(FieldDescriptor fs, bool bo) { Data[fs.Pos] = bo; }
        void IDataWriter.AsChar(FieldDescriptor fs, char ch) { Data[fs.Pos] = ch; }
        void IDataWriter.AsBytes(FieldDescriptor fs, byte[] bt) { Data[fs.Pos] = bt; }
        void IDataWriter.AsDate(FieldDescriptor fs, DateTime dt) { Data[fs.Pos] = dt; }
        void IDataWriter.AsDecimal(FieldDescriptor fs, decimal de) { Data[fs.Pos] = de; }
        void IDataWriter.AsDouble(FieldDescriptor fs, double dl) { Data[fs.Pos] = dl; }
        void IDataWriter.AsFloat(FieldDescriptor fs, float dl) { Data[fs.Pos] = dl; }
        void IDataWriter.AsInt(FieldDescriptor fs, int iv) { Data[fs.Pos] = iv; }
        void IDataWriter.AsLong(FieldDescriptor fs, long lv) { Data[fs.Pos] = lv; }
        void IDataWriter.AsString(FieldDescriptor fs, string s) { Data[fs.Pos] = s; }
        void IDataWriter.AsCurrency(FieldDescriptor fs, Currency cy) { Data[fs.Pos] = cy; }
        void IDataWriter.AsEnum(FieldDescriptor fs, int en) { Data[fs.Pos] = en; }
        void IDataWriter.AsSi32(FieldDescriptor fs, int i) { Data[fs.Pos] = i; }
        void IDataWriter.AsSi64(FieldDescriptor fs, long l) { Data[fs.Pos] = l; }
        void IDataWriter.AsMessage(FieldDescriptor fs, Message msg)
        {
            var prev = Data;
            var ci = fs.MessageType;
            Data = new object[ci.FieldCount];
            msg.Put(this);
            if (fs == null) return;
            prev[fs.Pos] = Data;
            Data = prev;
        }
        void IDataWriter.AsRepeated(FieldDescriptor fs, Array data)
        {
            throw new NotImplementedException();
        }
    }

    public class StringReader : IDataReader
    {
        public string Value { get; set; }
        public long AsBit64() { return Text.ParseHex(Value, 0, Value.Length); }
        public bool AsBool() { return bool.Parse(Value); }
        public byte[] AsBytes() { return Convert.FromBase64String(Value); }
        public Currency AsCurrency() { return new Currency(Double.Parse(Value)); }
        public DateTime AsDate() { return DateTime.ParseExact(Value, Text.ISODateFormat, null); }
        public decimal AsDecimal() { return Decimal.Parse(Value); }
        public double AsDouble() { return double.Parse(Value); }
        public int AsInt() { return int.Parse(Value); }
        public long AsLong() { return long.Parse(Value); }
        public void AsMessage(Message msg, FieldDescriptor fs) { throw new NotSupportedException(); }
        public string AsString() { return Value; }
        public int AsBit32() { return AsInt(); }
        public char AsChar() { return Value[0]; }
        public int AsEnum(EnumDescriptor es) { throw new NotImplementedException(); }
        public float AsFloat() { return (float)AsDouble(); }
        public int AsSi32() { return AsInt(); }
        public long AsSi64() { return AsLong(); }
    }

    // base class for messages with single repeated message field.
    public class MessageArray<T> : Message where T : Message, new()
    {
        protected Repeated<T> _msgs;

        public MessageArray() { }
        public MessageArray(T[] init) { _msgs = init; }
        public int Count { get { return _msgs.Count; } }
        public T[] Items { get { return _msgs.Items; } }
        public T this[int i] { get { return _msgs[i]; } }
        public override void Clear() { _memoized_size = 0; _msgs.Clear(); }
        //public void Add(T ni) { _msgs.Add(ni); }
        public void AddRange(T[] ni, int pos, int count) { _msgs.AddRange(ni, pos, count); }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool Equals(object msg)
        {
            var test = msg as MessageArray<T>;
            if (test == null) return false;
            if (Count != test.Count) return false;
            for (var i = 0; i < Count; i++)
                if ((_msgs[i] == null && test[i] != null) || !_msgs[i].Equals(test[i])) return false;
            return true;
        }

        public override void Get(FieldDescriptor fs, IDataReader dr)
        {
            var ti = new T(); 
            dr.AsMessage(ti, fs); 
            _msgs.Add(ti);
        }

        public override int GetSerializedSize()
        {
            var sz = _msgs.Count;
            if(sz > 0)
                foreach(var item in _msgs.Items)
                    sz += Pbs.msg(item);
            return _memoized_size = sz;
        }

        public override bool IsInitialized()
        {
            var count = Count;
            if (count == 0 || !GetDescriptor().Fields[0].IsRequired)
                return true;
            foreach(var item in _msgs.Items) 
                if (!item.IsInitialized()) 
                    return false;
            return true;
        }

        public override void Put(IDataWriter dw)
        {
            dw.AsRepeated(GetDescriptor().Fields[0], _msgs.Items);
        }
    }

    // map<string,string> container class implementation.

    public class MapEntry : Message
    {
        public MessageDescriptor _desc;
        
        public long lk, lv;
        public string sk;
        public object ov;

        public MapEntry(MessageDescriptor ds) { _desc = ds; }

        // new/clear/etc not needed, get-size will be done by compiler up-level

        public override MessageDescriptor GetDescriptor() { return _desc; }

        internal void PutKey(IDataWriter dw)
        {
            PutValueEx(dw, _desc.Fields[0], lk, false);
        }

        internal void PutValue(IDataWriter dw)
        {
            PutValueEx(dw, _desc.Fields[1], lv, true);
        }

        protected void PutValueEx(IDataWriter dw, FieldDescriptor fs, long lval, bool is_value)
        {
            switch (fs.DataType)
            {
                case WireType.String: dw.AsString(fs, is_value ? ov as string : sk); break;
                case WireType.Message: dw.AsMessage(fs, ov as Message); break;
                case WireType.Bytes: dw.AsBytes(fs, ov as byte[]); break;
                case WireType.Decimal: dw.AsDecimal(fs, (decimal)ov); break;

                case WireType.Int32: dw.AsInt(fs, (int)lval); break;
                case WireType.Bit32: dw.AsBit32(fs, (int)lval); break;
                case WireType.Bool: dw.AsBool(fs, lval == 0 ? false : true); break;
                case WireType.Char: dw.AsChar(fs, (char)lval); break;
                case WireType.Sint32: dw.AsSi32(fs, (int)lval); break;
                case WireType.Enum: dw.AsEnum(fs, (int)lval); break;

                case WireType.Bit64: dw.AsBit64(fs, lval); break;
                case WireType.Sint64: dw.AsSi64(fs, lval); break;
                case WireType.Int64: dw.AsLong(fs, lval); break;
                case WireType.Currency: dw.AsCurrency(fs, new Currency(lval)); break;
                case WireType.Float: dw.AsFloat(fs, (float)Pbs.SetDoubleBits(lv)); break;
                case WireType.Double: dw.AsDouble(fs, Pbs.SetDoubleBits(lv)); break;
                case WireType.Date: dw.AsDate(fs, new DateTime(lv)); break;

                default: throw new SerializationException("map write - key type" );
            }
        }

        protected long GetValue(IDataReader dr, FieldDescriptor fs, bool is_value = false)
        {
            switch (fs.DataType)
            {
                case WireType.String: 
                    var s = dr.AsString();
                    if (is_value) ov = s; else sk = s;
                    break;
                case WireType.Bytes:
                    if (!is_value) goto default;
                    ov = dr.AsBytes(); break;
                case WireType.Message:
                    if (!is_value) goto default;
                    var msg = fs.MessageType.New();
                    dr.AsMessage(msg, fs);
                    ov = msg; break;
                case WireType.Decimal: 
                    ov = dr.AsDecimal(); break;

                case WireType.Bool: return dr.AsBool() ? 1 : 0;
                case WireType.Bit32: return dr.AsBit32();
                case WireType.Char: return dr.AsChar();
                case WireType.Sint32: return dr.AsSi32();
                case WireType.Int32: return dr.AsInt();

                case WireType.Bit64: return dr.AsBit64();
                case WireType.Sint64: return dr.AsSi64();
                case WireType.Int64: return dr.AsLong();

                case WireType.Currency: return dr.AsCurrency().Value;
                case WireType.Enum: return dr.AsEnum((EnumDescriptor)fs.MessageType);
                case WireType.Float: return (long)Pbs.GetDoubleBits((double)dr.AsFloat());
                case WireType.Double: return (long)Pbs.GetDoubleBits(dr.AsDouble()); 
                case WireType.Date: return dr.AsDate().Ticks;

                default: throw new SerializationException("map read - data type");
            }
            return 0;
        }

        protected int GetSize(WireType dtype, long value)
        {
            switch (dtype)
            {
                case WireType.String: return Pbs.str((string)ov);
                case WireType.Bytes: return Pbs.bts((byte[])ov);
                case WireType.Message: return Pbs.msg((Message)ov);
                case WireType.Decimal: return Pbs.dec((decimal)ov);
                case WireType.Bool: return 1;
                case WireType.Float:
                case WireType.Bit32: return 4;
                case WireType.Double:
                case WireType.Bit64: return 8;
                case WireType.Int32:
                case WireType.Enum:
                case WireType.Char: return Pbs.i32((int)value);
                case WireType.Date:
                case WireType.Currency:
                case WireType.Int64: return Pbs.i64(value);
                case WireType.Sint32: return Pbs.si32((int)value);
                case WireType.Sint64: return Pbs.si64(value);
                default: throw new SerializationException("map read - data type");
            }
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var test = obj as MapEntry;
            if (sk != null)
                if (sk != test.sk) return false; else { }
            else if (lk != test.lk) return false;
            if (ov == null)
                if (lv != test.lv) return false;
                else
                {
                    var msg = ov as Message;
                    if (msg != null)
                        if (!msg.Equals(test.ov)) return false; else { }
                    else
                    {
                        //TODO.
                    }
                }
            return true;
        }

        public override void Put(IDataWriter dw)
        {
            PutKey(dw);
            PutValue(dw);
        }

        public override int GetSerializedSize()
        {
            var fs = _desc.Fields;
            var sz = 2;
            if (sk != null) sz += Pbs.str(sk); else sz += GetSize(fs[0].DataType, lk);
            sz += GetSize(fs[1].DataType, lv);
            return _memoized_size = sz;
        }

        public override void Get(FieldDescriptor fs, IDataReader dr)
        {
            if (fs.Pos == 0) lk = GetValue(dr, fs, false);
            else lv = GetValue(dr, fs, true);
        }
    }

    // key-value map type used for bulk operations on proto-maps.
    public class KeyValueMap<TK,TV>
    {
        private Repeated<Entry<TK, TV>> _list;

        public class Entry<TKE, TVE>
        {
            public TKE Key { get; private set; }
            public TVE Value { get; private set; }
            public Entry(TKE key, TVE value) { Key = key; Value = value; }
        }

        public Entry<TK,TV> Add(TK key, TV value)
        {
            return _list.Add(new Entry<TK, TV>(key, value));
        }

        public int Count { get { return _list.Count; } }
        public Entry<TK, TV> this[int i] { get { return _list[i]; } }
    }
} 
