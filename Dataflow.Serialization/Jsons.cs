using System;

namespace Dataflow.Serialization
{
    public class JsonException : SerializationException
    {
        public JsonException(string s) : base(s) { }
    }

    public partial class Struct
    {
        internal MapEntry[] _get_fields() { return _fields.Items; }

        public Struct AddMember(string key, long value) { return AddFields(key, new Value { Bigint = value }); }
        public Struct AddMember(string key, double value) { return AddFields(key, new Value { Number = value }); }
        public Struct AddMember(string key, string value) { return AddFields(key, new Value { String = value }); }
        public Struct AddMember(string key, bool value) { return AddFields(key, new Value { Bool = value }); }
        public Struct AddMember(string key, Struct value) { return AddFields(key, new Value { Members = value }); }
        public Struct AddMember(string key, ListValue value) { return AddFields(key, new Value { Elements = value }); }
        public Struct AddMember(string key) { return AddFields(key, new Value()); }
    }

    internal enum JsToken
    {
        None = 0, Comma = 1, Semicolon = 2, ArrayEnd = 3, ObjectEnd = 4,
        ObjectStart = 5, ArrayStart = 6,
        String = 7, Number = 8, Int = 9, True = 10, False = 11, Null = 12
    }

    //
    // extracts messages from JSON format.
    //
    public class JSDataReader : IDataReader
    {
        private const int iCharBufSz = 128;

        private StorageReader _storage;
        private JsToken _tlast;
        private int _cp, _sp;

        // parser build-up buffer.
        private char[] _dc;
        private char _cc;

        // string source and current value/name.
        private readonly string _sdb;
        private string _last_name;
        private string _sdata;

        public JSDataReader(DataStorage ds)
        {
            _storage = new StorageReader(ds);
        }

        public JSDataReader(byte[] dt, int pos, int size)
        {
            _storage = new StorageReader(dt, pos, size);
        }

        public JSDataReader(string dt, int pos, int size)
        {
            _sdb = dt;
        }

        // reads message content in JSON format from the current position.
        public void Read(Message msg)
        {
            _dc = new char[iCharBufSz];
            _cc = GetNextCharJs();
            _tlast = Next();
            AsMessage(msg, msg.GetDescriptor());
        }

        #region JSON parsing data helpers.

        // read next number from json stream
        private JsToken NextNumber(char ch)
        {
            var token = JsToken.Int;
            // fill char buffer with number chars
            for (_dc[0] = ch, _cp = 1; _cc != 0; _cc = GetNextCharJs())
                if (_cc >= '0' && _cc <= '9')
                    _dc[_cp++] = _cc;
                // leading +- is processed by top level parser.
                else if (_cc != '.' && _cc != '-' && _cc != '+' && _cc != 'e' && _cc != 'E')
                    break;
                else
                {
                    _dc[_cp++] = _cc;
                    token = JsToken.Number;
                }
            // real number syntax check is on data extractors
            return token;
        }

        private JsToken NextString()
        {
            while (_cc != 0)
            {
                var ch = _cc;
                _cc = GetNextCharJs();
                if (ch == '"')
                {
                    var s = (_cp != 0) ? new string(_dc, 0, _cp) : null;
                    _sdata = _sdata == null ? s : _sdata + s;
                    return JsToken.String;
                }
                // detect escapes sequences.
                if (ch == '\\')
                {
                    switch (_cc)
                    {
                        case 'b': ch = '\b'; break;
                        case 'f': ch = '\f'; break;
                        case 'n': ch = '\n'; break;
                        case 'r': ch = '\r'; break;
                        case 't': ch = '\t'; break;
                        case '\\': ch = '\\'; break;
                        case '/': ch = '/'; break;
                        case '"': ch = '"'; break;
                        case 'u':
                            {
                                _cc = GetNextCharJs();
                                var cu = new[] { GetCharJs(), GetCharJs(), GetCharJs(), GetCharJs() };
                                ch = (char)Convert.ToInt32(new string(cu), 16);
                            } break;
                        default:
                            throw new JsonException("unknown escape sequence");
                    }
                    _cc = GetNextCharJs();
                }
                if (_cp < _dc.Length)
                    _dc[_cp++] = ch;
                else if(_dc.Length == iCharBufSz)
                {
                    Array.Resize<char>(ref _dc, iCharBufSz * 4);
                    _dc[_cp++] = ch;
                }
                else
                {
                    var s = new string(_dc);
                    _sdata = _sdata == null ? s : _sdata + s;
                    _cp = 1; _dc[0] = ch;
                }
            }
            // no " found before empty load, report as error.
            throw new JsonException("unexpected end of string");
        }

        private JsToken NextConst(char ch)
        {
            var nc = (uint)ch << 24;
            nc += ((uint)GetCharJs() << 16) + ((uint)GetCharJs() << 8) + (byte)GetCharJs();
            if (nc == 0x74727565)
                return JsToken.True;
            if (nc == 0x6E756C6C)
                return JsToken.Null;
            if (nc == 0x66616C73 && GetCharJs() == 'e')
                return JsToken.False;
            return JsToken.None;
        }

        private JsToken Next()
        {
            _cp = 0;
            _sdata = null;
            do
            {
                var ch = _cc;
                _cc = GetNextCharJs();
                if (ch <= ' ')
                    if (ch != 0) continue;
                    else break;
                switch (ch)
                {
                    case '{': return JsToken.ObjectStart;
                    case '}': return JsToken.ObjectEnd;
                    case '[': return JsToken.ArrayStart;
                    case ']': return JsToken.ArrayEnd;
                    case ',': return JsToken.Comma;
                    case ':': return JsToken.Semicolon;
                    case '-':
                    case '+': return NextNumber(ch);
                    case '"': return NextString();
                    case 'f':
                    case 'n':
                    case 't': return NextConst(ch);
                    default:
                        if (ch >= '0' && ch <= '9')
                            return NextNumber(ch);
                        return JsToken.None;
                }
            } while (true);
            return JsToken.None;
        }

        private string AtString()
        {
            return _sdata ?? (_cp > 0 ? new string(_dc, 0, _cp) : null);
        }

        private long GetLongJs()
        {
            if (_cp == 0)
            {
                if (_sdata == null) return 0;
                foreach (char c in _sdata) _dc[_cp++] = c;
            }

            var isneg = false;
            var bp = 0;
            if (_dc[bp] < '0') { isneg = _dc[bp] == '-'; bp++; }

            long val = 0, bs = 1;
            while (_cp > bp)
            {
                var di = _dc[--_cp] - '0';
                if ((uint)di > 9)
                    Expected("digit");
                val += di * bs; bs *= 10;
            }
            return isneg ? -val : val;
        }

        private Currency GetCurrencyJs()
        {
            if (_cp == 0)
            {
                if (_sdata == null) return 0;
                foreach (var c in _sdata) _dc[_cp++] = c;
            }

            for (var i = 0; i < _cp; i++)
                if (_dc[i] == '.')
                {
                    for (var k = 0; k < 4; k++) { i++; _dc[i - 1] = i < _cp ? _dc[i] : '0'; }
                    _cp = i;
                    break;
                }
            return new Currency(GetLongJs());
        }

        private char GetNextCharJs()
        {
            return _storage != null ? _storage.GetChar() : (_sp < _sdb.Length ? _sdb[_sp++] : '\0');
        }

        private char GetCharJs()
        {
            var ch = _cc;
            _cc = GetNextCharJs();
            return ch;
        }

        private JsToken Expected(string s)
        {
            throw new JsonException(s + " expected");
        }

        private JsToken GetValue()
        {
            return (int)(_tlast = Next()) >= (int)JsToken.ObjectStart ? _tlast : Expected("value");
        }

        #endregion

        #region IDataReader implementation

        public override long AsBit64() { return GetLongJs(); }

        public override bool AsBool()
        {
            if (_tlast == JsToken.True) return true;
            if (_tlast != JsToken.False)
                Expected("bool value");
            return false;
        }

        public override byte[] AsBytes()
        {
            return Convert.FromBase64String(_sdata);
        }

        public override char AsChar()
        {
            return _sdata != null ? _sdata[0] : (char)0;
        }

        public override Currency AsCurrency()
        {
            return GetCurrencyJs();
        }

        public override DateTime AsDate()
        {
            var sz = _sdata.Length;
            if (sz < 20) throw new JsonException("incorrect date format");
            var fmt = _sdata[sz - 4] == ':' ? "yyyy-MM-ddThh:mm:ssZ" : Text.ISODateFormat;
            var dt = DateTime.ParseExact(_sdata, fmt, System.Globalization.CultureInfo.InvariantCulture);
            return dt.ToLocalTime();
        }

        public override double AsDouble()
        {
            return double.Parse(AtString(), System.Globalization.NumberStyles.Float);
        }

        public override int AsEnum(EnumDescriptor es)
        {
            var ds = es.Find(AtString());
            if (ds == null) Expected("enum value");
            return ds.Id;
        }

        public override float AsFloat()
        {
            return float.Parse(AtString(), System.Globalization.NumberStyles.Float);
        }

        public override int AsInt()
        {
            return (int)GetLongJs();
        }

        public override long AsLong()
        {
            return GetLongJs();
        }

        public override string AsString()
        {
            return _sdata;
        }

        public void AsArray(Message msg, FieldDescriptor pos)
        {
            for (GetValue(); _tlast != JsToken.ArrayEnd; )
            {
                if (_tlast != JsToken.Null)
                    msg.Get(pos, this);
                if ((_tlast = Next()) == JsToken.Comma)
                    GetValue();
                else if (_tlast != JsToken.ArrayEnd && (int)_tlast < (int)JsToken.ObjectStart)
                    Expected("value");
            }
        }

        public override int AsBit32() { return (int)GetLongJs(); }
        public override int AsSi32() { return (int)GetLongJs(); }
        public override long AsSi64() { return GetLongJs(); }

        public override void AsMessage(Message msg, FieldDescriptor fs)
        {
            AsMessage(msg, fs.MessageType);
        }

        #endregion

        protected Value AsValueJs(Value data)
        {
            switch (_tlast)
            {
                case JsToken.String: data.String = AtString(); break;
                case JsToken.Int: data.Bigint = GetLongJs(); break;
                case JsToken.Number:
                    data.Number = double.Parse(AtString(), System.Globalization.NumberStyles.Float); break;
                case JsToken.False: data.Bool = false; break;
                case JsToken.True: data.Bool = true; break;
                case JsToken.Null: data.ClearKind(); break;
                case JsToken.ObjectStart: data.Members = AsStructJs(new Struct()); break;
                case JsToken.ArrayStart:
                    var list = new ListValue();
                    for (GetValue(); _tlast != JsToken.ArrayEnd;)
                    {
                        AsValueJs(list.AddValues(new Value()));
                        if ((_tlast = Next()) == JsToken.Comma)
                            GetValue();
                        else if (_tlast != JsToken.ArrayEnd && (int)_tlast < (int)JsToken.ObjectStart)
                            Expected("value");
                    }
                    data.Elements = list;
                    break;
                default:
                    Expected("json data element"); break;
            }
            return data;
        }

        protected Struct AsStructJs(Struct data)
        {
            if (_tlast != JsToken.ObjectStart)
                Expected("json object");

            var comma = false;
            while ((_tlast = Next()) != JsToken.ObjectEnd)
            {
                if (comma)
                    if (_tlast != JsToken.Comma) Expected("comma");
                    else _tlast = Next();
                else comma = true;
                if (_tlast != JsToken.String) Expected("name");
                _last_name = _sdata;
                if (Next() != JsToken.Semicolon) Expected(":");
                GetValue();
                var jdata = AsValueJs(new Value());
                data.AddFields(_last_name, jdata);
            }
            return data;
        }

        protected void AsMessageEx(Message msg, MessageDescriptor ci)
        {
            if (ci.IsGoogleType)
            {
                var type = msg.GetType();
                if (type == typeof(Struct))
                    AsStructJs(msg as Struct);
                else if (type == typeof(Value))
                    AsValueJs(msg as Value);
                else { }
            }
            else if (ci.IsKvMap)
            {
                var kmap = msg as MapEntry;
                var fs = ci.Fields;
                // set map entry key and value.
                if (fs[0].DataType == WireType.String) kmap.skey = _last_name;
                else { long lk = 0; if (long.TryParse(_last_name, out lk)) kmap.lkey = lk; else Expected("ordinal key value"); }
                var fi = fs[1];
                kmap.Get(fs[1], this);
            }
            else if (ci.IsBoxType)
            {
                if (!ci.IsListType) msg.Get(null, this);
                else AsArray(msg, null);
            }
        }

        protected void AsMessage(Message msg, MessageDescriptor ci)
        {
            if (ci.HasOptions)
            {
                AsMessageEx(msg, ci);
                return;
            }

            // should be ready to pick up value if parsing into Json data node special class.
            if (_tlast != JsToken.ObjectStart)
                Expected("object start");

            // parse JSON object into message fields.
            var comma = false;
            FieldDescriptor kvfs = null;
            while (true)
            {
                if ((_tlast = Next()) == JsToken.ObjectEnd)
                {
                    if (kvfs == null) break;
                    kvfs = null; continue;
                }
                if (!comma) comma = true;
                else
                    if (_tlast != JsToken.Comma) Expected("comma");
                    else _tlast = Next();
                if (_tlast != JsToken.String) Expected("name");
                _last_name = _sdata;
                if (Next() != JsToken.Semicolon) Expected(":");
                if (GetValue() == JsToken.Null) continue;

                // when parsing JSON object into map, populate next entry.
                if (kvfs != null)
                {
                    msg.Get(kvfs, this);
                    continue;
                }

                // find message field descriptor by name.
                var fs = ci.Find(_last_name);
                if (fs == null) continue;
                if (_tlast != JsToken.ArrayStart)
                    // save field desc for extracting kv-map values.
                    if (fs.DataType != WireType.MapEntry)
                        msg.Get(fs, this);
                    else if (_tlast == JsToken.ObjectStart)
                        { kvfs = fs; comma = false; }
                    else Expected("object start");
                else AsArray(msg, fs);
            }
        }
    }

    //
    // serialize messages into JSON format.
    //
    public class JSDataWriter : IDataWriter
    {
        private StorageWriter _storage;
        private int _nestlvl, _fieldPos;

        protected bool Decorate { get; private set; }
        public StorageWriter Storage { get { return _storage; } }
        public JSDataWriter(StorageWriter sw, bool decorate = false) { _storage = sw; Decorate = decorate; }

        public void Flush() { _storage.Flush(); }

        #region JSON streaming data helpers.

        private JSDataWriter RollComma(ref bool separate)
        {
            if (separate)
                _storage.WriteByte((byte)',');
            else separate = true;
            return this;
        }

        private void PutDecor()
        {
            _storage.WriteByte((byte)'\n');
            for (int i = 0; i < _nestlvl; i++) _storage.WriteByte((byte)'\t');
        }

        private void PutString(string s)
        {
            WriteQuote();
            var escapes = EscapeChars.Instance.Table;
            foreach (char c in s)
                if (c >= escapes.Length)
                    _storage.WriteUTF8Char(c);
                else 
                {
                    var b = escapes[c];
                    if (b == 0) _storage.WriteByte((byte)c);
                    else _storage.WriteByte((byte)'\\', b);
                }
            WriteQuote();
        }

        private StorageWriter PutPrefix(FieldDescriptor fi)
        {
            // was "if (fi.IsBoxed) {", now JSON Value carries that burden :).
            if (_fieldPos++ > 0) WriteComma();
            if (Decorate) PutDecor();
             PutString(fi.Name);
            WriteSemic();
            return _storage;
        }

        private void PutCurrency(Currency cy)
        {
            var cl = cy.Value;
            var units = cl / Currency.Scale;
            _storage.WriteLongAL(units);
            var cents = (int)(cl - units * Currency.Scale);
            if (cents == 0) return;
            WriteChar('.');
            for (int bc = 1000; cents > 0; bc /= 10)
            {
                int i = '0';
                while (cents >= bc) { cents -= bc; i++; }
                _storage.WriteByte((byte)i);
            }
        }

        private void PutDate(DateTime dt)
        {
            WriteQuote();
            WriteStringA(dt.ToUniversalTime().ToString(Text.ISODateFormat));
            WriteQuote();
        }

        private void PutDouble(double dv)
        {
            WriteStringA(dv.ToString("R"));
        }

        private void PutMapEntry(MapEntry item)
        {
            WriteQuote();
            if (item.skey != null)
                _storage.WriteString(item.skey);
            else _storage.WriteLongAL(item.lkey);
            WriteQuote();
            WriteSemic();
            item.PutValue(this);
        }

        private void PutValueJs(Value data)
        {
            var comma = false;
            switch (data.OneOfKind)
            {
                case Value.KindCase.Kind_NOT_SET: WriteStringA("null"); break;
                case Value.KindCase.String: PutString(data.String); break;
                case Value.KindCase.Bigint: _storage.WriteLongAL(data.Bigint); break;
                case Value.KindCase.Number: PutDouble(data.Number); break;
                case Value.KindCase.Bool: WriteStringA(data.Bool ? "true" : "false"); break;
                case Value.KindCase.Members: PutStructJs(data.Members); break;
                case Value.KindCase.Elements:
                    WriteChar('[');
                    foreach (var dta in data.Elements.Items)
                    {
                        if (comma) WriteComma(); else comma = true;
                        PutValueJs(dta);
                    }
                    WriteChar(']');
                    break;
            }
        }

        private void PutStructJs(Struct data)
        {
            var comma = false;
            WriteChar('{');
            foreach (var dtk in data._get_fields())
            {
                if (comma) WriteComma(); else comma = true;
                PutString(dtk.skey);
                WriteSemic();
                var dtx = dtk.ov as Value;
                if (dtx != null) PutValueJs(dtx);
                else WriteStringA("null");
            }
            WriteChar('}');
        }

        private void PutGoogleType(Message msg)
        {
            var type = msg.GetType();
            if (type == typeof(Struct)) PutStructJs(msg as Struct);
            else if (type == typeof(Value)) PutValueJs(msg as Value);
            //TODO : add support for more google 'built-in' types serialization.
            else { } 
        }

        public JSDataWriter AppendMessage(Message msg, MessageDescriptor ci)
        {
            // special cases may require custom JSON formatting.
            if (ci.HasOptions)
            {
                if (ci.IsGoogleType) PutGoogleType(msg);
                else if (ci.IsKvMap) PutMapEntry(msg as MapEntry);
                else msg.Put(this);
                return this;
            }

            // standard JSON object serialization.
            _nestlvl++;
            WriteChar('{');
            var keep_pos = _fieldPos; _fieldPos = 0;
            msg.Put(this);
            _fieldPos = keep_pos;
            _nestlvl--;
            if (Decorate) PutDecor();
            WriteChar('}');
            return this;
        }

        private void WriteStringA(string s) { _storage.WriteStringA(s); }
        private void WriteChar(char ch) { _storage.WriteByte((byte)ch); }
        private void WriteQuote() { _storage.WriteByte((byte)'"'); }
        private void WriteSemic() { _storage.WriteByte((byte)':'); }
        private void WriteComma() { _storage.WriteByte((byte)','); }

        #endregion

        #region IDataWriter implementation

        public override void AsRepeated(FieldDescriptor fs, Array data)
        {
            PutPrefix(fs);
            PutRepeated(fs, data);
        }

        private void PutRepeated(FieldDescriptor fs, Array data)
        {
            // protobuf maps require special treatment in JSON
            if(fs.DataType == WireType.MapEntry)
            {
                WriteChar('{');
                _nestlvl++;
                var pos = 0;
                var map = data as MapEntry[];
                foreach (var item in map)
                {
                    if (pos++ != 0) WriteComma();
                    if (Decorate) PutDecor();
                    PutMapEntry(item);
                }
                _nestlvl--;
                if (Decorate) PutDecor();
                WriteChar('}');
                return;
            }

            // serialize repeatable field as standard JSON array.
            WriteChar('[');
            var separate = false;
            switch (fs.DataType)
            {
                case WireType.Bool:
                    foreach (var bl in data as bool[]) RollComma(ref separate).WriteStringA(bl ? "true" : "false");
                    break;
                case WireType.Char:
                    foreach (var ch in data as char[])
                    {
                        RollComma(ref separate).WriteQuote();
                        _storage.WriteUTF8Char(ch);
                        WriteQuote();
                    }
                    break;
                case WireType.Enum:
                    var eds = (EnumDescriptor)fs.MessageType;
                    foreach (var ia in data as int[])
                    {
                        RollComma(ref separate);
                        var ds = eds.GetById(ia);
                        if (ds == null) throw new ArgumentException("enum value");
                        PutString(ds.Name);
                    }
                    break;
                case WireType.Bit32:
                case WireType.Int32:
                    foreach (var ia in data as int[]) { RollComma(ref separate); _storage.WriteIntAL(ia); }
                    break;
                case WireType.Bit64:
                case WireType.Int64:
                    foreach (var la in data as long[]) { RollComma(ref separate); _storage.WriteLongAL(la); }
                    break;
                case WireType.Currency:
                    foreach (var cr in data as Currency[]) RollComma(ref separate).PutCurrency(cr);
                    break;
                case WireType.Date:
                    foreach (var dt in data as DateTime[]) RollComma(ref separate).PutDate(dt);
                    break;
                case WireType.Double:
                    foreach (var da in data as double[]) RollComma(ref separate).WriteStringA(da.ToString("R"));
                    break;
                case WireType.Float:
                    foreach (var da in data as float[]) RollComma(ref separate).WriteStringA(da.ToString("R"));
                    break;
                case WireType.String:
                    foreach (var sa in data as string[]) RollComma(ref separate).PutString(sa);
                    break;
                case WireType.Bytes:
                    foreach (var bt in data as byte[][])
                    {
                        RollComma(ref separate).WriteQuote();
                        _storage.WriteBase64String(bt, 0, bt.Length);
                        WriteQuote();
                    }
                    break;
                case WireType.Message:
                    foreach (var ma in data as Message[])
                        if (ma != null)
                            RollComma(ref separate).AppendMessage(ma, fs.MessageType);
                    break;
                default: throw new NotSupportedException();
            }
            WriteChar(']');
        }

        public override void AsBit32(FieldDescriptor fs, int i)
        {
            PutPrefix(fs).WriteIntAL(i);
        }

        public override void AsBit64(FieldDescriptor fs, long l)
        {
            PutPrefix(fs).WriteLongAL(l);
        }

        public override void AsBool(FieldDescriptor fs, bool b)
        {
            PutPrefix(fs).WriteStringA(b ? "true" : "false");
        }

        public override void AsBytes(FieldDescriptor fs, byte[] bt)
        {
            PutPrefix(fs);
            WriteQuote();
            _storage.WriteBase64String(bt, 0, bt.Length);
            WriteQuote();
        }

        public override void AsChar(FieldDescriptor fs, char ch)
        {
            PutPrefix(fs);
            WriteQuote();
            _storage.WriteUTF8Char(ch);
            WriteQuote();
        }

        public override void AsCurrency(FieldDescriptor fs, Currency cy)
        {
            PutPrefix(fs);
            PutCurrency(cy);
        }

        public override void AsDate(FieldDescriptor fs, DateTime dt)
        {
            PutPrefix(fs);
            PutDate(dt);
        }

        public override void AsDouble(FieldDescriptor fs, double d)
        {
            PutPrefix(fs).WriteStringA(d.ToString("R"));
        }

        public override void AsEnum(FieldDescriptor fs, int e)
        {
            var eds = (EnumDescriptor)fs.MessageType;
            var ds = eds.GetById(e);
            if (ds == null) throw new ArgumentException("enum value");
            PutPrefix(fs);
            PutString(ds.Name);
        }

        public override void AsFloat(FieldDescriptor fs, float f)
        {
            PutPrefix(fs).WriteStringA(f.ToString("R"));
        }

        public override void AsInt(FieldDescriptor fs, int i)
        {
            PutPrefix(fs).WriteIntAL(i);
        }

        public override void AsLong(FieldDescriptor fs, long i)
        {
            PutPrefix(fs).WriteLongAL(i);
        }

        public override void AsSi32(FieldDescriptor fs, int i)
        {
            PutPrefix(fs).WriteIntAL(i);
        }

        public override void AsSi64(FieldDescriptor fs, long l)
        {
            PutPrefix(fs).WriteLongAL(l);
        }

        public override void AsString(FieldDescriptor fs, string s)
        {
            PutPrefix(fs);
            PutString(s);
        }

        public override void AsMessage(FieldDescriptor fs, Message msg)
        {
            PutPrefix(fs);
            AppendMessage(msg, fs.MessageType);
        }

        public override void IsNull(FieldDescriptor fs) { }

        #endregion
    }
}
