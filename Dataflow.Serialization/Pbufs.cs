/* 
 * Google Protocol Buffers format implementation, (c) Viktor Poteryakhin; Dataflow Software Inc, 2012-2015.
 */
using System;
using System.Text;

namespace Dataflow.Serialization
{

    public class ProtoBufException : SerializationException
    {
        public ProtoBufException(string s) : base(s) { }
    }

    /// <summary>
    /// Reads messages from a byte stream in the Google Protocol Buffers format.
    /// </summary>

    public sealed class PBDataReader : IDataReader
    {
        private const int MaxNestLevel = 80, MaxBlobSize = 0x800000; // +8M
        private StorageReader _storage;
        private int _wirefmt;     // wire format for current field 
        private int _level, _fsz; // current field byte size and nesting level
        private long _data;       // current field value for non-RLE types 

        public PBDataReader(DataStorage ds)
        {
            _storage = new StorageReader(ds);
        }

        public PBDataReader(byte[] bt, int pos, int size)
        {
            _storage = new StorageReader(bt, pos, size);
        }

        // reads message content from Google Protocol Buffers stream.
        public void Read(Message msg, int size)
        {
            if (size > 0)
            {
                _level = 0;
                // start protobuf stream parsing.
                _storage.Limit = _fsz = size;
                _wirefmt = Pbs.iString;
                GetMessage(msg, msg.GetDescriptor());
                // check if we parsed message to the limit.
                if (_storage.Limit > 0) 
                    throw new ProtoBufException("incomplete message data");
            }
            else if (size < 0) 
                throw new ArgumentException("message size");
        }

        private long FormatError(int wt) 
        { 
            if(wt >= 0)
                throw new ProtoBufException("type mismatch: expected " + Pbs.GetWireTypeName(wt) + ", actual " + Pbs.GetWireTypeName(_wirefmt));
            return 0;
        }

        private long GetVarInt()
        {
            return _wirefmt == Pbs.iVarInt ? _data : FormatError(Pbs.iVarInt);
        }

        private long GetBits64(int expected)
        {
            return _wirefmt == expected ? _data : FormatError(expected);
        }

        private void GetMessage(Message msg, MessageDescriptor ci)
        {
            // enter next message parsing level.
            var prev = PushLimit();
            while (_storage.Limit > 0)
            {
                // read next PB value from the stream.
                var id = _storage.GetIntPB();
                switch (_wirefmt = id & 0x7)
                {
                    case Pbs.iVarInt: _data = _storage.GetLongPB(); break;
                    case Pbs.iBit64: _data = _storage.GetB64(); break;
                    case Pbs.iString:
                        _fsz = _storage.GetIntPB();
                        if (_fsz <= _storage.Limit)
                            break;
                        throw new ProtoBufException("nested blob size");
                    case Pbs.iBit32: _data = _storage.GetB32(); break;
                    default: throw new ProtoBufException("unsupported wire format");
                }
                // match PB field descriptor by id.
                var fi = ci.Find(id);
                if (fi != null)
                    if(fi.Id == id)
                        msg.Get(fi, this);
                    else TryReadPacked(fi, msg);
                else
                    if (_fsz > 0) { _storage.Skip(_fsz); _fsz = 0; }
            }
            // exit message segment parsing.
            if (_storage.Limit < 0)
                throw new ProtoBufException("message size out of sync");
            _level--;
            PopLimit(prev);
        }

        #region DataReader interface implementation.

        public override int AsBit32()
        {
            return (int)GetBits64(Pbs.iBit32);
        }

        public override long AsBit64()
        {
            return GetBits64(Pbs.iBit64);
        }

        public override bool AsBool()
        {
            return GetVarInt() != 0;
        }

        public override byte[] AsBytes()
        {
            if (_wirefmt == Pbs.iString)
            {
                if (_fsz == 0) return null;
                if (_fsz <= MaxBlobSize)
                    return _storage.GetBytes(new byte[_fsz], ref _fsz);
                throw new ProtoBufException("bytes size");
            }
            FormatError(Pbs.iString);
            return null;
        }

        public override char AsChar()
        {
            return (char)GetVarInt();
        }

        public override Currency AsCurrency()
        {
            return new Currency(GetVarInt());
        }

        public override DateTime AsDate()
        {
            // using signed int encoding for date ticks encoding.
            var i = GetVarInt();
            return Pbs.DateFromMsecs((i >> 1) ^ -(i & 1));
        }

        public override double AsDouble()
        {
            return Pbs.SetDoubleBits(GetBits64(Pbs.iBit64));
        }

        public override int AsEnum(EnumDescriptor es)
        {
            var ev = (int)GetVarInt();
            // todo: should we check if it is valid enum value.
            return ev;
        }

        public override float AsFloat()
        {
            return Pbs.SetFloatBits((int)GetBits64(Pbs.iBit32));
        }

        public override int AsInt()
        {
            return (int)GetVarInt();
        }

        public override long AsLong()
        {
            return GetVarInt();
        }

        public override void AsMessage(Message message, FieldDescriptor fs)
        {
            if (_wirefmt == Pbs.iString)
            {
                if (_fsz == 0) return;
                if (++_level < MaxNestLevel)
                    GetMessage(message, fs.MessageType);
                else throw new ProtoBufException("message nesting too deep");
            }
            else FormatError(Pbs.iString);
        }

        public override string AsString()
        {
            if (_wirefmt != Pbs.iString) 
                FormatError(Pbs.iString);
            if (_fsz == 0) return null;
            if (_fsz > MaxBlobSize) throw new ProtoBufException("string size");
            var pos = _fsz;
            var bt = _storage.GetBytes(null, ref pos);
            var s = Encoding.UTF8.GetString(bt, pos, _fsz);
            _fsz = 0;
            return s;
        }

        public override int AsSi32()
        {
            var i = (int)GetVarInt();
            return (i >> 1) ^ -(i & 1);
        }

        public override long AsSi64()
        {
            var i = GetVarInt();
            return (i >> 1) ^ -(i & 1);
        }

        #endregion

        private void PopLimit(int prev) 
        {
            _storage.Limit = prev; 
        }

        private int PushLimit()
        {
            var prev = _storage.Limit;
            if (_fsz <= prev) _storage.Limit = _fsz;
            else throw new ProtoBufException("nested limit out of bounds");
            _fsz = 0;
            // number of bytes left after new limit will runs out.
            return prev - _storage.Limit;
        }

        public void TryReadPacked(FieldDescriptor fs, Message msg)
        {
            // since 2.3 PB deserializers are supposed to read both packed and unpacked.
            var wireFmt = fs.Id & 0x7;
            if (_wirefmt == Pbs.iString)
                _wirefmt = wireFmt;
            else FormatError(wireFmt);

            var prev = PushLimit();
            while (_storage.Limit > 0)
            {
                switch (_wirefmt)
                {
                    case Pbs.iVarInt: _data = _storage.GetLongPB(); break;
                    case Pbs.iBit64: _data = _storage.GetB64(); break;
                    case Pbs.iBit32: _data = _storage.GetB32(); break;
                    default: throw new ProtoBufException("packed: type");
                }
                msg.Get(fs, this);
            }
            PopLimit(prev);
        }
    }

    /// <summary>
    /// Writes messages to a stream in the Google Protocol Buffers encoding.
    /// </summary>
    public class PBDataWriter : IDataWriter
    {
        private StorageWriter _storage;
        public StorageWriter Storage { get { return _storage; } } 
        public PBDataWriter(StorageWriter sw) { _storage = sw; }
        public PBDataWriter(byte[] bts, int pos, int count) { _storage = new StorageWriter(bts, pos, count); }

        public PBDataWriter AppendMessage(Message message, MessageDescriptor ci)
        {
            // force recalc on _memoized_size to guarantee up-to-date value.
            message.GetSerializedSize();
            // serialize message fields.
            message.Put(this);
            return this;
        }

        #region DataWriter interface implementation

        private void WriteInt(FieldDescriptor fs, int i) 
        { 
            _storage.WriteIntPB(fs.Id, i);
        }

        private void WriteLong(FieldDescriptor fs, long l)
        {
            _storage.WriteIntPB(fs.Id);
            _storage.WriteLongPB(l);
        }

        private void WriteDate(DateTime dt)
        {
            var l = Pbs.DateToMsecs(dt);
            _storage.WriteLongPB((l << 1) ^ (l >> 63));
        }

        private void WriteMessage(FieldDescriptor fs, Message msg)
        {
            if (msg == null) return;
            _storage.WriteIntPB(fs.Id, msg.SerializedSize);
            msg.Put(this);
        }

        private void WriteString(FieldDescriptor fs, string s)
        {
            _storage.WriteIntPB(fs.Id, Pbs.GetUtf8ByteSize(s));
            _storage.WriteString(s);
        }

        private void AsRepeatedPacked(FieldDescriptor fs, Array data)
        {
            // replace wire type in field id to string/bytes.
            _storage.WriteIntPB((fs.Id & ~0x7) | Pbs.iString);
            var sz = 0;
            switch (fs.DataType)
            {
                case WireType.Enum:
                case WireType.Int32:
                    {
                        var ia = data as int[];
                        if (ia == null) goto default;
                        if (!fs.IsSignedInt)
                        {
                            foreach (var x in ia) sz += Pbs.i32(x);
                            _storage.WriteIntPB(sz);
                            foreach (var x in ia) _storage.WriteIntPB(x);
                        }
                        else
                        {
                            foreach (var x in ia) sz += Pbs.si32(x);
                            _storage.WriteIntPB(sz);
                            foreach (var x in ia) _storage.WriteIntPB((x << 1) ^ (x >> 31));
                        }
                    } break;
                case WireType.Bit32:
                    var b4 = data as int[];
                    if (b4 == null) goto default;
                    _storage.WriteIntPB(b4.Length * 4);
                    foreach (var x in b4) _storage.WriteB32((uint)x);
                    break;
                case WireType.Int64:
                    var la = data as long[];
                    if (la == null) goto default;
                    if (!fs.IsSignedInt)
                    {
                        foreach (var x in la) sz += Pbs.i64(x);
                        _storage.WriteIntPB(sz);
                        foreach (var x in la) _storage.WriteLongPB(x);
                    }
                    else
                    {
                        foreach (var x in la) sz += Pbs.si64(x);
                        _storage.WriteIntPB(sz);
                        foreach (var x in la) _storage.WriteLongPB((x << 1) ^ (x >> 63));
                    }
                    break;
                case WireType.Bit64:
                    var b8 = data as long[];
                    if (b8 == null) goto default;
                    _storage.WriteIntPB(b8.Length * 8);
                    foreach (var x in b8) _storage.WriteB64((ulong)x);
                    break;
                case WireType.Bool:
                    var bla = data as bool[];
                    if (bla == null) goto default;
                    _storage.WriteIntPB(bla.Length);
                    foreach (var x in bla) _storage.WriteIntPB(x ? 1 : 0);
                    break;
                case WireType.Char:
                    var ch = data as char[];
                    if (ch == null) goto default;
                    foreach (var x in ch) sz += Pbs.i32(x);
                    _storage.WriteIntPB(sz);
                    foreach (var x in ch) _storage.WriteIntPB(x);
                    break;
                case WireType.Currency:
                    var cra = data as Currency[];
                    if (cra == null) goto default;
                    foreach (var x in cra) sz += Pbs.i64(x.Value);
                    _storage.WriteIntPB(sz);
                    foreach (var x in cra) _storage.WriteLongPB(x.Value);
                    break;
                case WireType.Date:
                    var dta = data as DateTime[];
                    if (dta == null) goto default;
                    foreach (var x in dta) sz += Pbs.dat(x);
                    _storage.WriteIntPB(sz);
                    foreach (var x in dta) WriteDate(x);
                    break;
                case WireType.Double:
                    var da = data as double[];
                    if (da == null) goto default;
                    _storage.WriteIntPB(da.Length * 8);
                    foreach (var x in da) _storage.WriteB64((ulong)Pbs.GetDoubleBits(x));
                    break;
                case WireType.Float:
                    var fa = data as float[];
                    if (fa == null) goto default;
                    _storage.WriteIntPB(fa.Length * 4);
                    foreach (var x in fa) _storage.WriteB32((uint)Pbs.GetFloatBits(x));
                    break;
                default:
                    throw new ProtoBufException("packed: unsupported element type");
            }
        }

        private void AsRepeatedArray(FieldDescriptor fs, Array data)
        {
            IDataWriter writer = this;
            switch (fs.DataType)
            {
                case WireType.Enum:
                case WireType.Int32:
                    if(!fs.IsSignedInt)
                        foreach (var x in data as int[]) WriteInt(fs, x);
                    else foreach (var x in data as int[]) writer.AsSi32(fs, x);
                    break;
                case WireType.Bit32:
                    foreach (var x in data as int[]) writer.AsBit32(fs, x);
                    break;
                case WireType.Int64:
                    if (!fs.IsSignedInt)
                        foreach (var x in data as long[]) WriteLong(fs, x); 
                    else foreach (var x in data as long[]) writer.AsSi64(fs, x);
                    break;
                case WireType.Bit64:
                    foreach (var x in data as long[]) writer.AsBit64(fs, x);
                    break;
                case WireType.Bool:
                    foreach (var x in data as bool[]) WriteInt(fs, x ? 1 : 0);
                    break;
                case WireType.Char:
                    foreach (var x in data as char[]) WriteInt(fs, x);
                    break;
                case WireType.Currency:
                    foreach (var x in data as Currency[]) WriteLong(fs, x.Value);
                    break;
                case WireType.Date:
                    foreach (var x in data as DateTime[]) writer.AsDate(fs, x);
                    break;
                case WireType.Double:
                    foreach (var x in data as double[]) writer.AsDouble(fs, x);
                    break;
                case WireType.Float:
                    foreach (var x in data as float[]) writer.AsFloat(fs, x);
                    break;
                case WireType.String:
                    foreach (var x in data as string[]) WriteString(fs, x);
                    break;
                case WireType.Bytes:
                    foreach (var x in data as byte[][]) writer.AsBytes(fs, x);
                    break;
                case WireType.Message:
                    foreach (var x in data as Message[]) WriteMessage(fs, x);
                    break;
                case WireType.MapEntry:
                    foreach (var x in data as MapEntry[]) WriteMessage(fs, x);
                    break;
                default:
                    throw new ProtoBufException("repeated: unsupported or mismatched element type");
            }
        }

        public override void AsRepeated(FieldDescriptor fs, Array data)
        {
            try
            {
                if (!fs.IsPacked)
                    AsRepeatedArray(fs, data);
                else
                    AsRepeatedPacked(fs, data);
            }
            catch (Exception)
            {
                throw new ProtoBufException("unsupported array element type");
            }
        }

        public override void AsBit32(FieldDescriptor fs, int i)
        {
            _storage.WriteIntPB(fs.Id);
            _storage.WriteB32((uint)i);
        }

        public override void AsBit64(FieldDescriptor fs, long l)
        {
            _storage.WriteIntPB(fs.Id);
            _storage.WriteB64((ulong)l);
        }

        public override void AsBool(FieldDescriptor fs, bool b) { WriteInt( fs, b ? 1 : 0); }
        public override void AsChar(FieldDescriptor fs, char ch) { WriteInt(fs, ch); }

        public override void AsBytes(FieldDescriptor fs, byte[] bs)
        {
            if (bs == null) return;
            _storage.WriteIntPB(fs.Id);
            var sz = bs.Length;
            _storage.WriteIntPB(sz);
            if (sz != 0) _storage.WriteBytes(sz, bs);
        }

        public override void AsCurrency(FieldDescriptor fs, Currency dc)
        {
            WriteLong(fs, dc.Value);
        }

        public override void AsDate(FieldDescriptor fs, DateTime dt)
        {
            _storage.WriteIntPB(fs.Id);
            WriteDate(dt);
        }

        public override void AsDouble(FieldDescriptor fs, double v)
        {
            _storage.WriteIntPB(fs.Id);
            _storage.WriteB64((ulong)Pbs.GetDoubleBits(v));
        }

        public override void AsEnum(FieldDescriptor fs, int en) { WriteInt(fs, en); }
        public override void AsInt(FieldDescriptor fs, int v) { WriteInt(fs, v); }
        public override void AsLong(FieldDescriptor fs, long v) { WriteLong(fs, v); }

        public override void AsFloat(FieldDescriptor fs, float v)
        {
            _storage.WriteIntPB(fs.Id);
            _storage.WriteB32((uint)Pbs.GetFloatBits(v));
        }

        public override void AsMessage(FieldDescriptor fs, Message msg)
        {
            WriteMessage(fs, msg);
        }

        public override void AsString(FieldDescriptor fs, string s)
        {
            WriteString(fs, s);
        }

        public override void AsSi32(FieldDescriptor fs, int i)
        {
            WriteInt(fs, (i << 1) ^ (i >> 31));
        }

        public override void AsSi64(FieldDescriptor fs, long l)
        {
            WriteLong(fs, (l << 1) ^ (l >> 63));
        }

        public override void IsNull(FieldDescriptor fs) { }

        #endregion
    }
}
