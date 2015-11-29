using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Dataflow.Serialization;
using Dataflow.Remoting;
using System.Collections;
using Microsoft.SqlServer.Server;

namespace Dataflow.Sql
{

    public struct ConnectionParams
    {
        public string ConnectionString;
        public string DataSource;
        public string Password;
        public string UserId;
        private static string[] _keys = { "Data Source", "User Id", "Password" };

        public ConnectionParams(string cs)
        {
            ConnectionString = cs;
            DataSource = Password = UserId = "";
            try
            {
                for (int i = 0; i < cs.Length; i++)
                {
                    while (i < cs.Length && cs[i] <= ' ') i++;
                    int kpos = 0, vpos = 0;
                    foreach (string key in _keys)
                        if (string.CompareOrdinal(key, 0, cs, i, key.Length) != 0) { kpos++; continue; }
                        else
                        {
                            i += key.Length;
                            while (cs[i] <= ' ') i++;
                            if (cs[i] == '=')
                            {
                                while (cs[++i] <= ' ') ;
                                vpos = i;
                            }
                            break;
                        }
                    // find end of line or key separator.
                    while (i < cs.Length && cs[i] != ';') i++;
                    if (vpos == 0) continue;
                    int epos = i; while (cs[--epos] <= ' ') ;
                    if (vpos > epos) continue;
                    string s = cs.Substring(vpos, epos - vpos + 1);
                    switch (kpos) { case 0: DataSource = s; break; case 1: UserId = s; break; case 2: Password = s; break; }
                }
            }
            catch
            {
                // "eat" incorrect syntax causing errors ??
            }
        }
    }

    public class RecordSet : MessageArray<Message> { }

    public class SqlSig : Signal
    {
        public enum Options { None = 0, DeadCon = 1 }
        public Options Flags;
        public const int iOpenConnection = 5, iExecute = 6, iFetch = 7;
        public bool ConnectionDead { get { return (Flags & SqlSig.Options.DeadCon) != 0; } set { if (value) Flags |= SqlSig.Options.DeadCon; else Flags &= ~SqlSig.Options.DeadCon; } }
        public SqlSig(string msg) : base(msg) { }
    }

    // base SQL connection class.
    public abstract partial class DbsConnection : Connection
    {
        protected SqlContext _pcache;
        protected int _max_pcache;

        public bool AutoCommit { get { return (_options & Option.AutoCommit) != 0; } }
        public string ConnectionString { get; protected set; }

        public DbsConnection(string cs) : base(null)
        {
            ConnectionString = cs;
            _max_pcache = 16;
        }

        protected virtual void ClearCached() { _pcache = null; }
        public abstract SqlContext CreateContext(SqlScript script, int boost);
        public abstract void BeginTransaction(IsolationLevel level);
        public abstract void EndTransaction(bool commit);

        protected SqlContext Find(SqlScript ss, int boost)
        {
            SqlContext ctx = _pcache;
            if (ctx == null || !ss.KeepPrepared) return null;
            if (ctx.Script == ss && ctx.CanBoost(boost))
            {
                _pcache = ctx.Next;
                return ctx;
            }
            //for (var prev = ctx; (ctx = ctx.Next) != null; prev = ctx)
            //    if (ctx.Script == ss && ctx.CanBoost(boost))
            //        return prev.ExtractNext();
            return null;
        }

        public AwaitIo ExecuteScript(SqlScript script, Message args, Message rets)
        {
            var boost = 0;
            var ctx = Find(script, boost);
            if (ctx != null) ctx.UpdateBinds(args, rets);
            else
            {
                ctx = CreateContext(script, boost);
                ctx.CreateBinds(args, rets);
                script.Compile(ctx);
            }
            return ExecuteContext(ctx, args, rets);
        }

        protected virtual AwaitIo ExecuteContext(SqlContext context, Message args, Message rets) { throw new NotSupportedException(); }
    }

    #region -- SQL Parameter binding and value conversion

    public enum SqlParamKind { Input, Output, InOut, Result };

    public class SqlPBind : IDataWriter
    {
        public FieldDescriptor Field { get; internal set; }
        public int Position { get; internal set; }
        public string Name { get; private set; }

        protected void ConversionError(string s) { }
        protected void NotSupported(Type from) { throw new DataException(string.Format("Conversion from type {0} into type {1} not supported.", from.Name, GetType().Name.Substring(5))); }

        public void BindValue(Message args)
        {
            args.PutField(this, Field);
        }
        public virtual SqlPBind NewInstance() { throw new NotImplementedException(); }

        public override void AsString(FieldDescriptor fs, string s) { NotSupported(typeof(string)); }
        public override void AsInt(FieldDescriptor fs, int i) { NotSupported(typeof(int)); }
        public override void AsLong(FieldDescriptor fs, long l) { NotSupported(typeof(long)); }
        public override void AsBytes(FieldDescriptor fs, byte[] bt) { NotSupported(typeof(byte[])); }
        public override void AsDate(FieldDescriptor fs, DateTime dt) { NotSupported(typeof(DateTime)); }
        public override void AsDouble(FieldDescriptor fs, double d) { NotSupported(typeof(double)); }
        public override void AsBool(FieldDescriptor fs, bool b) { NotSupported(typeof(bool)); }
        public override void AsChar(FieldDescriptor fs, char ch) { NotSupported(typeof(char)); }
        public override void AsEnum(FieldDescriptor fs, int en) { NotSupported(typeof(int)); }
        public override void AsFloat(FieldDescriptor fs, float f) { NotSupported(typeof(float)); }
        public override void AsCurrency(FieldDescriptor fs, Currency cy) { NotSupported(typeof(Currency)); }

        public override void AsRepeated(FieldDescriptor fs, Array data) { throw new NotImplementedException(); }
        public override void AsMessage(FieldDescriptor fs, Message msg) { throw new NotImplementedException(); }
        public override void IsNull(FieldDescriptor fs) { throw new NotImplementedException(); }
    }

    #endregion

    #region -- SQL Field binding and value conversion

    public class SqlFBind : Dataflow.Serialization.IDataReader
    {
        //public const string sBoolSet = "YyTtrue";
        public FieldsBinder Binder { get; set; }
        public FieldDescriptor Field { get; protected set; }
        public string Name { get; protected set; }
        public int Offset;

        public virtual WireType Wtype { get { return WireType.None; } }

        protected void ConvertFail(WireType wt)
        {
            throw new ArgumentException("can't convert value for column " + Name + " to type " + wt.ToString());
        }

        public override byte[] AsBytes() { throw new NotImplementedException(); }
        public override string AsString() { throw new NotSupportedException(); }
        public override int AsInt() { throw new NotSupportedException(); }
        public override long AsLong() { throw new NotSupportedException(); }
        public override double AsDouble() { throw new NotSupportedException(); }
        public override DateTime AsDate() { throw new NotSupportedException(); }

        public override bool AsBool() { return AsInt() != 0; }
        public override char AsChar() { var s = AsString(); return s != null ? s[0] : '\0'; }
        public override float AsFloat() { return (float)AsDouble(); }
        public override void AsMessage(Message msg, FieldDescriptor fs) { throw new NotImplementedException(); }
        public override Currency AsCurrency() { throw new NotImplementedException(); }
        public override int AsEnum(EnumDescriptor es) { throw new NotImplementedException(); }
    }

    public class SqlFString : SqlFBind
    {
        protected string Value { get { return Binder.Reader.GetString(Offset); } }
        public override WireType Wtype { get { return WireType.String; } }

        public sealed override string AsString() { return Value; }
        public sealed override byte[] AsBytes() { return Encoding.UTF8.GetBytes(Value); }
        public sealed override int AsInt()
        {
            var val = 0;
            if (!int.TryParse(Value, out val)) ConvertFail(WireType.Int32);
            return val;
        }
        public sealed override long AsLong()
        {
            long val = 0;
            if (!long.TryParse(Value, out val)) ConvertFail(WireType.Int64);
            return val;
        }
        public sealed override double AsDouble()
        {
            double val = 0;
            if (!double.TryParse(Value, out val)) ConvertFail(WireType.Double);
            return val;
        }
        public sealed override DateTime AsDate()
        {
            DateTime val = DateTime.MinValue;
            if (!DateTime.TryParse(Value, out val)) ConvertFail(WireType.Date);
            return val;
        }
    }

    public class SqlFInt32 : SqlFBind
    {
        protected int Value { get { return Binder.Reader.GetInt32(Offset); } }
        public override WireType Wtype { get { return WireType.Int32; } }
        public sealed override string AsString() { return Value.ToString(); }
        public sealed override int AsInt() { return Value; }
        public sealed override long AsLong() { return Value; }
        public sealed override double AsDouble() { return Value; }
    }

    public class SqlFInt64 : SqlFBind
    {
        protected long Value { get { return Binder.Reader.GetInt64(Offset); } }
        public override WireType Wtype { get { return WireType.Int64; } }
        public sealed override string AsString() { return Value.ToString(); }
        public sealed override int AsInt() { return (int)Value; }
        public sealed override long AsLong() { return Value; }
        public sealed override double AsDouble() { return Value; }
        public sealed override DateTime AsDate() { return new DateTime(Value); }
    }

    public class SqlFDouble : SqlFBind
    {
        protected double Value { get { return Binder.Reader.GetDouble(Offset); } }
        public override WireType Wtype { get { return WireType.Double; } }
        public sealed override string AsString() { return Value.ToString(); }
        public sealed override int AsInt() { return (int)Value; }
        public sealed override long AsLong() { return (long)Value; }
        public sealed override double AsDouble() { return Value; }
    }

    public class SqlFDecl : SqlFBind
    {
        protected double Value { get { return (double)Binder.Reader.GetDecimal(Offset); } }
        public override WireType Wtype { get { return WireType.Double; } }
        public sealed override string AsString() { return Value.ToString(); }
        public sealed override int AsInt() { return (int)Value; }
        public sealed override long AsLong() { return (long)Value; }
        public sealed override double AsDouble() { return (double)Value; }
    }

    public class SqlFDate : SqlFBind
    {
        public override WireType Wtype { get { return WireType.Date; } }
        public sealed override DateTime AsDate() { return Binder.Reader.GetDateTime(Offset); }
        public sealed override string AsString() { return AsDate().ToString(); }
    }

    public class SqlFBytes : SqlFBind
    {
        public override WireType Wtype { get { return WireType.Bytes; } }
        public sealed override byte[] AsBytes()
        {
            long bsize = Binder.Reader.GetBytes(Offset, 0, null, 0, 0);
            if (bsize > 0x10000) throw new Exception("bytes size too big");
            byte[] bt = new byte[bsize];
            long rsize = Binder.Reader.GetBytes(Offset, 0, bt, 0, (int)bsize);
            if (rsize != bsize) throw new ArgumentException("byte[] size mismatch");
            return bt;
        }
        public sealed override string AsString() { return Encoding.UTF8.GetString(AsBytes()); }
    }

    #endregion

    // params and fields binders for all scenarios.

    public class ParamsBinder
    {
        private SqlPBind[] _binds;
        private Message _value;
        private int _count;
        public MessageDescriptor Descriptor { get; private set; }

        public void BindValues(Message args)
        {
            foreach (var bind in _binds) bind.BindValue(args);
        }

        public void BindBatch(Message[] batch)
        {
            var pos = 0;
            foreach (var args in batch)
            {
                if (pos == _count) throw new ArgumentException("too many items in batch");
                for (var i = 0; i > _count; i++)
                    _binds[pos++].BindValue(args);
            }
        }
    }

    public class FieldsBinder
    {
        //private static Dictionary<Type, >
        protected SqlFBind[] _fields;
        protected object _value;

        public MessageDescriptor Descriptor { get; protected set; }
        public DbDataReader Reader { get; protected set; }
        public bool Empty { get { return _fields == null; } }
        public object SqlValue { get { return _value; } }
        public bool IsNullValue {  get { return _value == null; } }

        public virtual Message GetValues() { return null; }

        public void Describe(DbDataReader reader, MessageDescriptor desc)
        {
            Descriptor = desc;
            Reader = reader;
            var fcount = reader.FieldCount;
            if (fcount == 0) throw new ArgumentException("no fields to bind");
            if (_fields == null)
            {
                var mcount = 0;
                var ls = new SqlFBind[fcount];
                for (var i = 0; i < fcount; i++)
                {
                    var fname = reader.GetName(i);
                    var field = desc.Find(fname);
                    if (field == null) continue;
                    ls[mcount++] = CreateField(reader.GetFieldType(i));
                }
            }
        }

        protected SqlFBind CreateField(Type ctype)
        {
            SqlFBind sfi = null;
            if (ctype == typeof(string)) sfi = new SqlFString();
            else if (ctype == typeof(int)) sfi = new SqlFInt32();
            else if (ctype == typeof(double)) sfi = new SqlFDouble();
            else if (ctype == typeof(DateTime)) sfi = new SqlFDate();
            else if (ctype == typeof(long)) sfi = new SqlFInt64();
            else if (ctype == typeof(decimal)) sfi = new SqlFDecl();
            //else if( ctype == typeof( Boolean ) ) sfi.type = WireType.Bool;
            //else if( ctype == typeof( Single ) ) sfi.type = WireType.Float;
            else if (ctype == typeof(byte[])) sfi = new SqlFBytes();
            //else SerializationException.NotImplemented(WireType.None);
            return sfi;
        }

        public bool DescribeFields(int fcnt)
        {
            /*
            if (fcnt == 0) return false;
            SqlFBind list = null, last = Fields.Binds;
            // treat "pre-generated" metadata case.
            if (last != null)
            {
                int pos = 0;
                // describe will throw if existing bind if not compatible.
                do { DescribeField(pos++, last); } while ((last = last.Next) != null);
                return false;
            }
            for (int i = 0; i < fcnt; i++)
            {
                SqlFBind ns = DescribeField(i, null);
                if (list == null) last = list = ns;
                else last.Next = ns;
                last = ns;
            }
            return Fields.SetBinds(list);
            */
            return false;
        }

        protected Message ReadInto(Message rets)
        {
            foreach (var fs in _fields)
                rets.Get(fs.Field, fs);
            return rets;
        }

        protected Message ReadList(RecordSet rset)
        {
            var rets = Descriptor.New();
            ReadInto(rets);
            //rset.A.Add(rets);
            return rets;
        }

        public void FetchRow()
        {
        }
}

    public class FieldsBinderSingle : FieldsBinder
    {
        public Message Result;
        public FieldsBinderSingle(MessageDescriptor desc) { Descriptor = desc; }
        public override Message GetValues() { return ReadInto(Result); }
    }

    public class FieldsListBinder : FieldsBinder
    {
        protected RecordSet _list;
        public FieldsListBinder(RecordSet rets) { _list = rets; } //_desc = rets.RecordDescriptor; }
        public override Message GetValues() { return ReadList(_list); }
    }

 /*
    public class FieldsMapBinder : FieldsListBinder
    {
        protected class KeyField
        {
            public SqlFBind bind;
            public FieldDescriptor desc;
            public KeyField next;
        }
        protected ParamsWriter _pars;
        protected SqlBind _cols;
        protected KeyField _kmap;
        protected Message _key;

        public FieldsMapBinder(RecordSet rset, ParamsWriter ps, SqlBind cols)
            : base(rset)
        {
            if (cols == null) throw new ArgumentNullException();
            _cols = cols;
            _pars = ps;
            _key = ps.Descriptor.New();
        }
        public override Message GetValues()
        {
            // load key values from current row.
            _key.Clear();
            for (KeyField kf = _kmap; kf != null; kf = kf.next)
                _key.Get(kf.desc, kf.bind);
            // match key instance to requests group.
            int pos = _pars.Find(_key);
            if (pos < 0) throw new KeyNotFoundException();
            // read values into all targets inside group.
            Message rets = ReadPos(pos);
            int[] dd = null;// _pars.Dups;
            if (dd != null) while ((pos = dd[pos]) != 0) rets = ReadPos(pos);
            return rets;
        }
        internal Message ReadPos(int pos)
        {
            Message dest = _list[pos];
            RecordSet rset = dest as RecordSet;
            if (rset == null) return ReadInto(dest);
            else return ReadList(rset);
        }
        public override bool SetBinds(SqlFBind ls)
        {
            if (base.SetBinds(ls)) return true;
            FieldDescriptor pds = null;//TODO:fix _pars.Descriptor.Fields;
            for (SqlBind cc = _cols; cc != null; cc = cc.Next)
            {
                bool bound = false;
                for (SqlFBind fs = ls; fs != null; fs = fs.Next)
                    if (cc.Column.Name == fs.Name)
                    {
                        _kmap = new KeyField() { bind = fs, desc = pds, next = _kmap };
                        //pds = pds.Next;
                        bound = true;
                        break;
                    }
                if (!bound) throw new SerializationException("key match field not bound");
            }
            return false;
        }
    }
*/
    // SQL execution context, a-la statement.

    public abstract class SqlContext
    {
        protected int _boost;
        protected int _fetch_limit;
        // prepared contexts cache management.

        public SqlContext Next { get; protected set; }

        // compact context options support.
        [Flags]
        public enum Option { Prepared, SetParams, Eof, MapByName, BoostEq }
        protected Option Options;

        public bool Eof { get { return Options.HasFlag(Option.Eof); } }
        public bool Prepared { get { return Options.HasFlag(Option.Prepared); } }
        public bool ParamsByName { get { return Options.HasFlag(Option.MapByName); } }
        public bool IsNonQuery { get { return Fields == null; } }

        public int Boost { get { return _boost; } }
        public FieldsBinder Fields { get; private set; }
        public ParamsBinder Params { get; private set; }
        public SqlScript Script { get; private set; }

        public SqlContext(SqlScript script, int boost)
        {
            Script = script;
            _boost = boost;
            _fetch_limit = int.MaxValue;
        }
        public abstract SqlPBind CreateParam(FieldDescriptor fs, string name);
        public void CreateBinds(Message args, Message rets)
        {/*
            SqlBind keys = null;
            _params = args != Message.Empty ? new ParamsWriter(args) : null;
            keys = _script.GetKeys();
            // create fields bindings.
            if (rets == Message.Empty) return;
            RecordSet rset = rets as RecordSet;
            if (rset == null) _fields = new FieldsOneBinder(rets);
            else if (keys == null)
                _fields = new FieldsListBinder(rset);
            else _fields = new FieldsMapBinder(rset, _params, keys);*/
        }

        protected SqlFBind CreateField(Type ctype)
        {
            SqlFBind sfi = null;
            if (ctype == typeof(string)) sfi = new SqlFString();
            else if (ctype == typeof(Int32)) sfi = new SqlFInt32();
            else if (ctype == typeof(Double)) sfi = new SqlFDouble();
            else if (ctype == typeof(DateTime)) sfi = new SqlFDate();
            else if (ctype == typeof(Int64)) sfi = new SqlFInt64();
            else if (ctype == typeof(Decimal)) sfi = new SqlFDecl();
            //else if( ctype == typeof( Boolean ) ) sfi.type = WireType.Bool;
            //else if( ctype == typeof( Single ) ) sfi.type = WireType.Float;
            else if (ctype == typeof(byte[])) sfi = new SqlFBytes();
            //else SerializationException.NotImplemented(WireType.None);
            return sfi;
        }

        protected virtual SqlFBind DescribeField(int i, SqlFBind prev) { return null; }
        public bool DescribeFields(int fcnt)
        {/*
            if (fcnt == 0) return false;
            SqlFBind list = null, last = Fields.Binds;
            // treat "pre-generated" metadata case.
            if (last != null)
            {
                int pos = 0;
                // describe will throw if existing bind if not compatible.
                do { DescribeField(pos++, last); } while ((last = last.Next) != null);
                return false;
            }
            for (int i = 0; i < fcnt; i++)
            {
                SqlFBind ns = DescribeField(i, null);
                if (list == null) last = list = ns;
                else last.Next = ns;
                last = ns;
            }
            return Fields.SetBinds(list); */
            return false;
        }
        public bool CanBoost(int boost) { return boost == _boost || (_boost > 0 && boost < _boost && !Options.HasFlag(Option.BoostEq)); }
        public void Complete() { }
        public virtual async Task<int> Execute(Message args, Message rets) { await AwaitIo.Done; throw new NotImplementedException(); }
        public virtual char GetMarker(bool pars = false) { return !pars ? '"' : '@'; }
        public void UpdateBinds(Message args, Message rets)
        {
            //if (_params != null) _params.Update(args);
        }
    }

    // SQL Provider class, db-specific settings and configuration point.

    public class SqlProvider
    {
        [Flags]
        public enum Option { DmlBatch = 1 }
        protected Option Options;
        protected SqlPBind[] ParamTypes { get; set; }

        public string ProviderName { get; protected set; }
        public char Quote { get; protected set; }
        public char Marker { get; protected set; }
        public bool DmlBatching { get { return Options.HasFlag(Option.DmlBatch); } }

        public SqlProvider(char marker) { Marker = marker; Quote = '\''; }
        public virtual DbsConnection CreateConnection(string cs) { throw new NotImplementedException(); }

        public SqlPBind BindParam(FieldDescriptor fs)
        {
            var bi = ParamTypes[(int)fs.DataType];
            if (bi == null) throw new ArgumentException("parameter type");
            var ni = bi.NewInstance();
            ni.Field = fs;
            return ni;
        }
    }

    public class AdoNet : SqlProvider
    {
        public static AdoNet MySql, Odbc, Sqlite, SqlServer;

        private DbProviderFactory factory;
        public DbProviderFactory Factory { get { return factory ?? (factory = DbProviderFactories.GetFactory(ProviderName)); } }

        protected void BindParamTypes(SqlPBind ps, params DataType[] types)
        {
            foreach (var type in types) ParamTypes[(int)type] = ps;
        }

        static AdoNet()
        {
            Odbc = new AdoNet("System.Data.Odbc", '?');

            // initialize Ado.Net parameter types bindings.
            Odbc.BindParamTypes(new AdoPBString(), DataType.String);
            Odbc.BindParamTypes(new AdoPBRaw(), DataType.Bytes);
            Odbc.BindParamTypes(new AdoPBLong(), DataType.Int64, DataType.SInt64, DataType.Fixed64);
            Odbc.BindParamTypes(new AdoPBInt(), DataType.Int32, DataType.SInt32, DataType.Fixed32);
            Odbc.BindParamTypes(new AdoPBDouble(), DataType.Double);
            Odbc.BindParamTypes(new AdoPBDate(), DataType.Date);
            Odbc.BindParamTypes(new AdoPBCurrency(), DataType.Currency);

            // create rest of well known Ado.Net providers.
            MySql = new AdoNet("MySql.Data.MySqlClient", '@') { Quote = '"', ParamTypes = Odbc.ParamTypes };
            Sqlite = new AdoNet("System.Data.SQLite", '@') { ParamTypes = Odbc.ParamTypes };
            SqlServer = new AdoNet("System.Data.SqlClient", '@') { ParamTypes = Odbc.ParamTypes };
        }

        public AdoNet(string pname, char marker) : base(marker)
        {
            ProviderName = pname;
            ParamTypes = new SqlPBind[(int)DataType.LastIndex];
        }

        public override DbsConnection CreateConnection(string cs)
        {
            return new AdoConnection(cs, this);
        }
    }

    // ADO.NET specific classes.

    public class AdoPBind : SqlPBind
    {
        internal IDbDataParameter db;

        public override void IsNull(FieldDescriptor fs)
        {
            db.Value = DBNull.Value;
        }
    }

    #region ADO.NET parameter bindings.

    public class AdoPBDate : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBDate(); }
        public override void AsDate(FieldDescriptor fs, DateTime dt) { db.Value = dt; }
        public override void AsLong(FieldDescriptor fs, long l) { db.Value = new DateTime(l); }
        public override void AsString(FieldDescriptor fs, string s)
        {
            var value = DateTime.MinValue;
            if (DateTime.TryParse(s, out value))
                db.Value = value;
            else ConversionError(s);
        }
    }

    public class AdoPBDouble : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBDouble(); }
        public override void AsDouble(FieldDescriptor fs, double d) { db.Value = d; }
        public override void AsInt(FieldDescriptor fs, int i) { db.Value = (double)i; }
        public override void AsLong(FieldDescriptor fs, long l) { db.Value = (double)l; }
        public override void AsCurrency(FieldDescriptor fs, Currency cy) { db.Value = cy.ToDouble(); }
        public override void AsString(FieldDescriptor fs, string s)
        {
            var value = Double.MinValue;
            if (double.TryParse(s, out value))
                db.Value = value;
            else ConversionError(s);
        }
    }

    public class AdoPBInt : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBInt(); }
        public override void AsInt(FieldDescriptor fs, int i) { db.Value = i; }
        public override void AsChar(FieldDescriptor fs, char ch) { db.Value = (int)ch; }
        public override void AsDouble(FieldDescriptor fs, double d) { db.Value = (int)d; }
        public override void AsLong(FieldDescriptor fs, long l) { db.Value = (int)l; }
        public override void AsString(FieldDescriptor fs, string s)
        {
            int value;
            if (int.TryParse(s, out value))
                db.Value = value;
            else ConversionError(s);
        }
    }

    public class AdoPBLong : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBLong(); }
        public override void AsInt(FieldDescriptor fs, int i) { db.Value = i; }
        public override void AsDouble(FieldDescriptor fs, double d) { db.Value = (long)d; }
        public override void AsCurrency(FieldDescriptor fs, Currency cy) { db.Value = cy.Value; }
        public override void AsString(FieldDescriptor fs, string s)
        {
            long value;
            if (long.TryParse(s, out value))
                db.Value = value;
            else ConversionError(s);
        }
    }

    public class AdoPBCurrency : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBCurrency(); }
        public override void AsCurrency(FieldDescriptor fs, Currency cy) { db.Value = cy.ToDouble(); }
        public override void AsInt(FieldDescriptor fs, int i) { db.Value = new Currency(i, 0); }
        public override void AsDouble(FieldDescriptor fs, double d) { db.Value = d; }
        public override void AsLong(FieldDescriptor fs, long l) { db.Value = (double)l; }
        public override void AsString(FieldDescriptor fs, string s) { } // { if (!value.TryParse(s)) ConvertError(); }
    }

    public class AdoPBString : AdoPBind
    {
        public string Format { get; set; }
        public override SqlPBind NewInstance() { return new AdoPBString(); }
        public override void AsString(FieldDescriptor fs, string s) { db.Value = s; }
        public override void AsBytes(FieldDescriptor fs, byte[] bt) { db.Value = Encoding.UTF8.GetString(bt); }
        public override void AsChar(FieldDescriptor fs, char ch) { db.Value = ch < 128 ? Text.Singles[ch] : ch.ToString(); }
        public override void AsDate(FieldDescriptor fs, DateTime d) { db.Value = d.ToString(Format); }
        public override void AsInt(FieldDescriptor fs, int i) { db.Value = i.ToString(); }
        public override void AsDouble(FieldDescriptor fs, double d) { db.Value = d.ToString(); }
        public override void AsLong(FieldDescriptor fs, long l) { db.Value = l.ToString(); }
        public override void AsCurrency(FieldDescriptor fs, Currency cy) { db.Value = cy.ToString(); }
    }

    public class AdoPBRaw : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBRaw(); }
        public override void AsBytes(FieldDescriptor fs, byte[] bt) { db.Value = bt; }
    }

    public class AdoPBBool : AdoPBind
    {
        public override SqlPBind NewInstance() { return new AdoPBBool(); }
        public override void AsBool(FieldDescriptor fs, bool b) { db.Value = b; }
        public override void AsInt(FieldDescriptor fs, int i) { db.Value = (i != 0); }
        public override void AsString(FieldDescriptor fs, string s)
        {
            if (s == "") { db.Value = false; return; }
            var fc = s[0];
            db.Value = !((fc == 'f' || fc == 'F') && (s.Length == 1 || (s.Length == 5 && s.ToLower() == "false")));
        }
    }

    #endregion

    public class AdoConnection : DbsConnection
    {
        protected DbConnection _connection;
        public AdoNet Provider { get; private set; }

        public DbTransaction Transaction { get; private set; }
        public AdoConnection(string cs, AdoNet provider) : base(cs) { Provider = provider; }
        public DbCommand CreateCommand() { return _connection.CreateCommand(); }
        public override SqlContext CreateContext(SqlScript script, int boost) { return new AdoCommand(script, this, boost); }

        public override void BeginTransaction(IsolationLevel level)
        {
            if (Transaction != null) throw new InvalidOperationException("in transaction");
            Transaction = _connection.BeginTransaction(level);
        }

        public override void EndTransaction(bool commit)
        {
            if (Transaction == null) throw new InvalidOperationException("not in transaction");
            if (commit) Transaction.Commit();
            else Transaction.Rollback();
            Transaction = null;
        }

        public override AwaitIo ConnectAsync()
        {
            if (_connection != null && _connection.State == ConnectionState.Open)
                return AwaitIo.Done;
            if (_connection != null) ClearCached();
            _connection = Provider.Factory.CreateConnection();
            try
            {
                _connection.ConnectionString = ConnectionString;
                _connection.Open();
            }
            catch (Exception)
            {
                try { _connection.Dispose(); }
                catch { }
                _connection = null;
                throw;
            }
            return AwaitIo.Done;
        }

        public override AwaitIo CloseAsync(bool panic)
        {
            if (_connection != null)
            {
                ClearCached();
                // protective way to enforce closing.
                var dbx = _connection; _connection = null;
                try { dbx.Close(); }
                catch { }
            }
            return AwaitIo.Done;
        }
    }

    public class AdoCommand : SqlContext
    {
        private DbCommand _command;
        public AdoConnection Connection { get; private set; }

//        public sealed override string SQL { get { return _command.CommandText; } set { _command.CommandText = value; _command.CommandType = CommandType.Text; } }

        public AdoCommand(SqlScript script, AdoConnection dbc, int boost)
            : base(script, boost)
        {
            Connection = dbc;
            _command = Connection.CreateCommand();
            _command.Transaction = Connection.Transaction; //todo: is it necessary ???
        }

        public override SqlPBind CreateParam(FieldDescriptor fs, string name)
        {
            var dbp = _command.CreateParameter();
            // fill parameter attributes from field meta.
            dbp.ParameterName = name;
            dbp.Direction = ParameterDirection.Input;
            dbp.DbType = GetDbType(fs.DataType);
            var dtsz = fs.DataSize;
            if (dtsz == 0)
                switch (fs.DataType)
                {
                    case WireType.String: dtsz = 256; break;
                    case WireType.Bytes: dtsz = 8000; break;
                }
            dbp.Size = dtsz;
            // create new sql parameter binder and add .
            var ps = Connection.Provider.BindParam(fs) as AdoPBind;
            ps.Position = _command.Parameters.Count;
            ps.db = dbp;
            _command.Parameters.Add(dbp);
            return ps;
        }

        protected override SqlFBind DescribeField(int i, SqlFBind prev)
        {/*
            var ctype = _reader.GetFieldType(i);
            if (prev == null)
            {
                SqlFBind sfi = null;
                if (ctype == typeof(string)) sfi = new SqlFString();
                else if (ctype == typeof(Int32)) sfi = new SqlFInt32();
                else if (ctype == typeof(Double)) sfi = new SqlFDouble();
                else if (ctype == typeof(DateTime)) sfi = new SqlFDate();
                else if (ctype == typeof(byte[])) sfi = new SqlFBytes();
                else if (ctype == typeof(Int64)) sfi = new SqlFInt64();
                else if (ctype == typeof(Decimal)) sfi = new SqlFDecl();
                //else if( ctype == typeof( Single ) ) sfi.type = WireType.Float;
                //else if( ctype == typeof( Boolean ) ) sfi.type = WireType.Bool;
                else SerializationException.NotImplemented(WireType.None);
                // configure ADO field.
                sfi.Reader = _reader;
                sfi.Offset = i;
                sfi.Name = _reader.GetName(i);
                return sfi;
            }
            switch (prev.Wtype)
            {
                case WireType.String: if (ctype == typeof(string)) break; goto default;
                case WireType.Int32: if (ctype == typeof(int)) break; goto default;
                case WireType.Bytes: if (ctype == typeof(byte[])) break; goto default;
                default: return null;
            }
            prev.Reader = _reader; */
            return prev;
        }

        protected DbType GetDbType(WireType wt)
        {
            switch (wt)
            {
                case WireType.String: return DbType.String;
                case WireType.Bytes: return DbType.Binary;
                case WireType.Int32:
                case WireType.Sint32: return DbType.Int32;
                case WireType.Int64:
                case WireType.Sint64: return DbType.Int64;
                case WireType.Float:
                case WireType.Double: return DbType.Double;
                case WireType.Date: return DbType.DateTime;
                case WireType.Char: return DbType.String;
                case WireType.Bool: return DbType.Boolean;
                default: return DbType.String;
            }
        }

        public override async Task<int> Execute(Message args, Message rets)
        {
            if (Params != null)
            {
                if (args == null) throw new ArgumentException("params values required");
                Params.BindValues(args);
            }
 
            var rows = 0;
            if (Fields == null)
            {
                // for queries that are not expected to produce no result-sets.
                rows = await _command.ExecuteNonQueryAsync();
                var rcnt = rets as Int32Value;
                if (rcnt != null) rcnt.Value = rows;
            }
            else
            {
                // execute as reader for fetching results.
                DbDataReader reader = null;
                try
                {
                    reader = _command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        Fields.Describe(reader, null);
                        var limit = _fetch_limit == 0 ? 2000000 : _fetch_limit;
                        while (await reader.ReadAsync())
                        {
                            Fields.FetchRow();
                            if (++rows >= limit) return rows;
                        }
                    }
                }
                finally
                {
                    if (reader != null) reader.Close();
                }
            }
            // either number of records affected by DML or fetched by select.
            return rows;
        }

    }

    /*
     * Microsoft SQL Server connection and command classes.
     */

    public class SqlPBTable : AdoPBind
    {
        protected FieldDescriptor[] _fields;
        protected SqlMetaData[] _columns;
        protected SqlDataRecord _curdr;
        protected int _curci;

        protected void SetMapEntries(MapEntry[] ls, SqlDataRecord[] tvp) { }
        protected void SetMessages(Message[] ls, SqlDataRecord[] tvp)
        {
            var pos = 0;
            foreach (var msg in ls)
            {
                _curdr = tvp[pos++];
                for (_curci = 0; _curci < _fields.Length; _curci++)
                {
                    msg.PutField(this, _fields[_curci]);
                }
            }
        }

        #region redirect value writers to SqlDataRecord instead of DbParameter of base class.

        public override void AsBit32(FieldDescriptor fs, int i) { _curdr.SetInt32(_curci, i); }
        public override void AsInt(FieldDescriptor fs, int i) { _curdr.SetInt32(_curci, i); }
        public override void AsBit64(FieldDescriptor fs, long l) { _curdr.SetInt64(_curci, l); }
        public override void AsLong(FieldDescriptor fs, long l) { _curdr.SetInt64(_curci, l); }
        public override void AsString(FieldDescriptor fs, string s) { _curdr.SetString(_curci, s); }

        #endregion

        public override void AsRepeated(FieldDescriptor fs, Array data)
        {
            var tvp = new SqlDataRecord[data.Length];
            for (int i = 0; i < tvp.Length; i++) tvp[i] = new SqlDataRecord(_columns);

            var pos = 0;
            switch (fs.DataType)
            {
                case WireType.Enum:
                case WireType.Bit32:
                case WireType.Int32: foreach (var ia in data as int[]) tvp[pos++].SetInt32(0, ia); break;
                case WireType.Bit64:
                case WireType.Int64: foreach (var la in data as long[]) tvp[pos++].SetInt64(0, la); break;
                case WireType.MapEntry:
                case WireType.Message: SetMessages(data as Message[], tvp); break;
                case WireType.Currency: foreach (var cr in data as Currency[]) tvp[pos++].SetInt64(0, cr.Value); break; //todo: currency
                case WireType.Date: foreach (var dt in data as DateTime[]) tvp[pos++].SetDateTime(0, dt); break;
                case WireType.Double: foreach (var da in data as double[]) tvp[pos++].SetDouble(0, da); break;
                case WireType.Float: foreach (var da in data as float[]) tvp[pos++].SetFloat(0, da); break;
                case WireType.Bool: foreach (var bl in data as bool[]) tvp[pos++].SetBoolean(0, bl); break;
                case WireType.Char: foreach (var ch in data as char[]) tvp[pos++].SetChar(0, ch); break;
                case WireType.String: foreach (var sa in data as string[]) tvp[pos++].SetString(0, sa); break;
                case WireType.Bytes: foreach (var bt in data as byte[][]) tvp[pos++].SetBytes(0, 0, bt, 0, bt.Length); break;
                default: throw new NotSupportedException();
            }
            db.Value = tvp;
        }
    }

    public partial class SqlServerConnection : AdoConnection
    {
        private SqlTransaction _transact;
        private SqlConnection _dbc;
        internal SqlDataReader _reader, _rdnext;

        public SqlConnection Handle { get { return _dbc; } }
        public SqlServerConnection(string cs) : base(cs, AdoNet.SqlServer) { }
        protected void EndExecute()
        {
            if (_reader == null) return;
            try { _reader.Close(); } catch { }
            _reader = null;
        }
        public sealed override SqlContext CreateContext(SqlScript script, int boost) { return new SqlServerCommand(script, this, boost) { Dbc = this }; }

        public sealed override void BeginTransaction(IsolationLevel level)
        {
            if (_transact != null) throw new InvalidOperationException("in transaction");
            _transact = _dbc.BeginTransaction(level);
        }

        public sealed override void EndTransaction(bool commit)
        {
            if (_transact == null) throw new InvalidOperationException("not in transaction");
            if (commit) _transact.Commit();
            else _transact.Rollback();
            _transact = null;
        }

        public sealed override AwaitIo ConnectAsync()
        {
            if (_dbc != null && _dbc.State == ConnectionState.Open)
                return AwaitIo.Done;
            if (_dbc != null) ClearCached();
            _dbc = new SqlConnection(ConnectionString);
            try
            {
                _dbc.OpenAsync();
            }
            catch
            {
                CloseAsync(false);
                throw;
            }
            return _awaitable;
        }

        public sealed override AwaitIo CloseAsync(bool panic)
        {
            if (_dbc != null)
            {
                ClearCached();
                var dbx = _dbc; _dbc = null;
                try { dbx.Close(); } catch { }
            }
            return AwaitIo.Done;
        }
    }

    public class SqlServerCommand : AdoCommand
    {
        internal SqlServerConnection _dbc;
        protected SqlCommand _command;
        public SqlServerConnection Dbc { get { return _dbc; } set { _dbc = value; _command = _dbc.Handle.CreateCommand(); } }
        public SqlCommand Handle { get { return _command; } }
        public SqlServerCommand(SqlScript script, SqlServerConnection dbc, int boost) : base(script, dbc, boost) { }
        /*
                public override SqlPBind CreateParam(FieldDescriptor fs, string name)
                {
                    // generate unuque and short param name.
                    var marker = '@';
                    sb.Append(marker);
                    if (ParamsByName && sp != null)
                    {
                        sb.Append(sb.GetParamName(sp.pos));
                        return null;
                    }
                    var name = sb.GetParamName(sb.pcount);
                    sb.Append(name);
                    var sdt = GetSqlType(bind.Column.type);// GetSqlType( bind.Field.DataType );
                    var dtsz = bind.Column.Size;
                    if (dtsz == 0)
                        if (sdt == SqlDbType.VarChar) dtsz = 128;
                        else if (sdt == SqlDbType.Binary) dtsz = 2000;
                    var fi = new SqlParameter(name, sdt, dtsz) { Direction = ParameterDirection.Input };
                    // create new sql parameter binders.
                    _command.Parameters.Add(fi);
                    if (sp == null)
                        sp = CreateParam(bind.Column.type);// bind.Field.DataType );
                    // append new bind to the list.
                    sb.AppendBind(new AdoPBind(sp, fi));
                    return sp;
                }
        */
        protected SqlDbType GetSqlType(WireType wt)
        {
            switch (wt)
            {
                case WireType.String: return SqlDbType.VarChar;
                case WireType.Bytes: return SqlDbType.Binary;
                case WireType.Int32:
                case WireType.Sint32: return SqlDbType.Int;
                case WireType.Int64:
                case WireType.Sint64: return SqlDbType.BigInt;
                case WireType.Float:
                case WireType.Double: return SqlDbType.Float;
                case WireType.Date: return SqlDbType.DateTime;
                case WireType.Char: return SqlDbType.Char;
                case WireType.Bool: return SqlDbType.Bit;
                default: return SqlDbType.VarChar;
            }
        }
    }

}

