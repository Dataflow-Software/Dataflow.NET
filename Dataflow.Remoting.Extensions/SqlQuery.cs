using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Dataflow.Serialization;
using Dataflow.Remoting;

namespace Dataflow.Sql
{
    public class SqlError
    {
        public string Message;
        public SqlError(string s) { Message = s; }
    }


    #region SQL elements
    public abstract class SqlElement
    {
        public SqlElement Next;
        internal virtual Sql.Builder Output(Sql.Builder sb) { return sb; }
        public override string ToString()
        {
            return "";// Output( new Sql.Builder() ).ToString();
        }
    }

    public class SqlText : SqlElement
    {
        public string Text;
        internal sealed override Sql.Builder Output(Sql.Builder sb)
        {
            sb.Append(Text);
            return sb;
        }
    }

    public class SqlCondition : SqlElement
    {
        public string Ops;
        public string FieldName, ParamName;
        internal override Sql.Builder Output(Sql.Builder sb)
        {
            sb.Append(FieldName, Ops);
            //sb.AppendParam( ParamName, 0 );
            return sb;
        }
    }

    public class SqlConditions : SqlCondition
    {
        public Repeated<SqlCondition> List;
        public SqlConditions Add(SqlCondition sfc) { List.Add(sfc); return this; }
        internal override Sql.Builder Output(Sql.Builder sb)
        {
            if (List.Count < 2)
            {
                if (List.Count == 0) return sb;
                List[0].Output(sb);
            }
            else
            {
                for (int i = 0; i < List.Count; i++)
                {
                    if (i > 0)
                        sb.Append(')').Append(Ops);
                    sb.Append('(');
                    List[i].Output(sb);
                }
                sb.Append(')');
            }
            return sb;
        }
    }
    #endregion
    public static class Sql
    {
        public static SqlText Text(string text) { return new SqlText() { Text = text }; }

        // sql condition operators.
        public static SqlCondition Eq(string fname, string pname)
        {
            return new SqlCondition() { Ops = "=", FieldName = fname, ParamName = pname };
        }
        public static SqlCondition Op(string fname, string ops, string pname)
        {
            return new SqlCondition() { Ops = ops, FieldName = fname, ParamName = pname };
        }
        public static SqlConditions Or()
        {
            return new SqlConditions() { Ops = "or" };
        }
        public static SqlConditions And()
        {
            return new SqlConditions() { Ops = "and" };
        }

        public class Builder : TextBuilder
        {
            public SqlContext Context;
            public MessageDescriptor desc;
            internal Repeated<SqlError> errors;
            internal Repeated<SqlPBind> binds;
            internal int psingle, pcount;

            public Builder(SqlContext ctx) : base() { Context = ctx; }
            public SqlSig GetErrors() { return null; }
            public Sql.Builder SelectList(SqlBind _outs)
            {
                if (_outs == null) Append('*');
                else
                {
                    int pos = 0;
                    for (SqlBind sc = _outs; sc != null; sc = sc.Next)
                    {
                        if (pos++ != 0) Comma();
                        string s = sc.Column.Name;
                        Append(s);
                        if (s != sc.Field.Name) Space().Append(sc.Field.Name);
                    }
                }
                return this;
            }

            public virtual void AppendParam(string name, SqlColumn sc)
            {
                // match parameter marker to message field.
                var ds = desc.Find(name);
                if (ds == null)
                    if (!desc.IsBoxType)
                    {
                        errors.Add(new SqlError("param name not found"));
                        return;
                    }
                    else ds = desc.Fields[0];
                //AppendParam(new SqlBind(sc, ds));
            }

            public virtual void AppendValue(object value) { throw new NotImplementedException(); }
            public string GetTableAlias() { return "t"; }
            public string GetParamName(int pos)
            {
                const int ab = 'a';
                var sa = Dataflow.Serialization.Text.Singles;
                if (pos < 26) return sa[pos + ab];
                pos -= 26;
                if (pos < 256)
                    return sa[(pos >> 4) + ab] + sa[(pos & 15) + ab];
                if (pos > 4095) throw new ArgumentException("too many parameters");
                pos -= 256;
                var cc = pos >> 8; pos &= 255;
                return sa[cc + ab] + sa[(pos >> 4) + ab] + sa[(pos & 15) + ab];
            }
        }
    }

    // metadata driven query designer.

    public class SqlExpression
    {
        protected const int PriorityOr = 2, PriorityAnd = 3, PriorityValue = 15;
        public string ops;
        public int priority;

        public SqlExpressionLogic And(SqlExpression sep) { return new SqlExpressionLogic(this, sep, true); }
        public SqlExpressionLogic Or(SqlExpression sep) { return new SqlExpressionLogic(this, sep, false); }
        public SqlExpression Repeat(int cnt) { return new SqlExpressionRepeat(this, cnt); }

        internal virtual void Output(Sql.Builder sb) { }
    }

    // represents data value based sql expression.
    public class SqlExpressionData : SqlExpression
    {
        public string pretext;
        public SqlColumn left, right;
        public object value;
        public SqlExpressionData(string op, int pr) { ops = op; priority = pr; }
        internal sealed override void Output(Sql.Builder sb)
        {
            if (pretext == null)
                if (left != null) sb.Append(left.Name);
                else { /* some expressions like  .. exists have no left side */ }
            else sb.Append(pretext);
            sb.Append(ops);
            //if( value == null )
            //    sb.AppendParam( right.name, right.size );
            //else sb.AppendValue( value );
        }
    }

    // constructs logical sql expressions.
    public class SqlExpressionLogic : SqlExpression
    {
        public SqlExpression left, right;
        public SqlExpressionLogic(SqlExpression l, SqlExpression r, bool conjunction)
        {
            left = l; right = r;
            if (conjunction) { priority = PriorityAnd; ops = "and"; }
            else { priority = PriorityOr; ops = "or"; }
        }
        internal sealed override void Output(Sql.Builder sb)
        {
            if (left.priority >= priority)
                left.Output(sb);
            else
            {
                sb.Append('(');
                left.Output(sb);
                sb.Append(')');
            }
            // insert sql operator text.
            sb.Space().Append(ops).Space();
            // wrap right expression, if needed.
            if (right.priority >= priority)
                right.Output(sb);
            else
            {
                sb.Append('(');
                right.Output(sb);
                sb.Append(')');
            }
        }
    }

    // repeated inner expression specified number of times. 
    public class SqlExpressionRepeat : SqlExpression
    {
        private int count;
        private SqlExpression expr;
        public SqlExpressionRepeat(SqlExpression sep, int cnt) { expr = sep; count = cnt; priority = PriorityOr; ops = "or"; }
        internal sealed override void Output(Sql.Builder sb)
        {
            if (count < 2 || expr.priority > priority)
                for (int i = 0; i < count; i++)
                {
                    if (i > 0) sb.Append(' ').Append(ops).Append(' ');
                    expr.Output(sb);
                }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    if (i > 0) sb.Append(')').Append(ops);
                    sb.Append('(');
                    expr.Output(sb);
                }
            }
        }
    }

    public class LookupExpression : SqlExpression
    {
        protected SqlBind _keys;
        public SqlBind Keys { get { return _keys; } }
        public LookupExpression(SqlBind keys) { _keys = keys; }

        protected Sql.Builder add_key(Sql.Builder sb, SqlBind bi)
        {
            sb.Append(bi.Column.Name, "=");
            //sb.AppendParam(bi);
            return sb;
        }
        internal void OutputEx(Sql.Builder sb)
        {
            if (_keys.Next == null) { add_key(sb, _keys); return; }
            int pos = 0;
            for (SqlBind sc = _keys; sc != null; sc = sc.Next)
            {
                sb.Append(pos++ == 0 ? "(" : "and(");
                add_key(sb, sc);
                sb.Append(')');
            }
        }
        internal override void Output(Sql.Builder sb)
        {
            int boost = sb.Context.Boost;
            if (boost < 2) { OutputEx(sb); return; }
            sb.psingle = 1;
            if (_keys.Next == null)
            {
                sb.Append(_keys.Column.Name, " in (");
                for (int i = 0; i < boost; i++)
                {
                    if (i > 0) sb.Comma();
                    //sb.AppendParam(_keys);
                }
                sb.Append(')');
                return;
            }
            for (SqlBind ls = _keys; (ls = ls.Next) != null; sb.psingle++) ;
            sb.Append('(');
            for (int i = 0; i < boost; i++)
            {
                if (i > 0) sb.Append(")or(");
                OutputEx(sb);
            }
            sb.Append('(');
        }
    }

    public class SqlBind
    {
        internal SqlBind Next;
        protected SqlColumn _column;
        protected FieldDescriptor _field;
        public SqlColumn Column { get { return _column; } }
        public FieldDescriptor Field { get { return _field; } }
        public SqlBind(SqlColumn col, FieldDescriptor desc) { _column = col; _field = desc; }
    }

    public class SqlBinds
    {
        protected SqlBind _list, _last;
        protected MessageDescriptor _desc;
        public SqlBind List { get { return _list; } }
        public SqlBinds(MessageDescriptor desc)
        {
            _desc = desc;
        }
        public SqlBind Add(SqlColumn col, string name)
        {
            FieldDescriptor desc = _desc.Find(name);
            if (desc == null) throw new KeyNotFoundException(name);
            return Add(col, desc);
        }
        public SqlBind Add(SqlColumn col, FieldDescriptor desc)
        {
            SqlBind bind = new SqlBind(col, desc);
            if (_last == null) _list = bind;
            else _last.Next = bind;
            return _last = bind;
        }
    }

    public class SqlColumn
    {
        protected string _name;
        public WireType type;
        public string Name { get { return _name; } }
        public int Size;

        public SqlColumn() { }
        public SqlColumn(WireType wt, string name, int size) { type = wt; _name = name; Size = size; }

        public void Parse(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                type = WireType.String;
                Size = 128;
                return;
            }
            int pos = s.IndexOf(":");
            string dt = s; s = null;
            if (pos > 0)
            {
                s = pos + 1 < dt.Length ? dt.Substring(pos + 1) : null;
                dt = dt.Substring(0, pos);
            }
            switch (dt)
            {
                case "int": type = WireType.Int32; break;
                case "long": type = WireType.Int64; break;
                case "date": type = WireType.Date; break;
                case "number": type = WireType.Double; break;
                case "string": type = WireType.String; break;
                case "bytes": type = WireType.Bytes; break;
                case "money": type = WireType.Currency; break;
                default: type = WireType.String; break;
            }
            if (s != null) Size = int.Parse(s);
        }
        public SqlExpression Like(string val) { return new SqlExpressionData("LIKE", 5) { left = this, value = val }; }
        public SqlExpression Eq(object val) { return new SqlExpressionData("=", 5) { left = this, value = val }; }
        public SqlExpression Eq(SqlColumn col) { return new SqlExpressionData("=", 5) { left = this, right = col }; }
        public SqlExpression Expr(string ex, object val) { return new SqlExpressionData(ex, 4) { left = this, value = val }; }
        public SqlExpression Expr(string ex, SqlColumn col) { return new SqlExpressionData(ex, 4) { left = this, right = col }; }
    }

    // specific SQL query types.

    public partial class SqlScript : ServiceMethod
    {
        [Flags]
        public enum Option
        {
            None = 0, HasParams = 1, HasResult = 2, CanBatch = 4, PureText = 8,
            StoredProc = 16, KeepPrepared = 32, SingleRow = 64, GetMatch = 128
        }
        //public int QueuedCount;
        protected string _sql;
        protected int _boost, _tag;

        protected Option _opts;
        public bool CanBatch { get { return (_opts & Option.CanBatch) != 0; } }
        public bool IsStoredProc { get { return (_opts & Option.StoredProc) != 0; } }
        public bool KeepPrepared { get { return (_opts & Option.KeepPrepared) != 0; } }
        public bool SingleRow { get { return (_opts & Option.SingleRow) != 0; } }
        public Option Options { get { return _opts; } }

        public int Boost { get { return _boost; } set { if (value < 0 || value == 1) throw new ArgumentException("boost out of range"); _boost = value; } }
        public string SQL { get { return _sql; } set { _sql = value; } }
        public int Tag { get { return _tag; } }

        public SqlScript() : base() { }

        public void Compile(SqlContext ctx)
        {
            if ((Options & Option.PureText) != 0)
            {
                //ctx.SQL = SQL;
                return;
            }
            // text is collected into sb, params/fields into context.
            SqlSig errors = null;
            var sb = new Sql.Builder(ctx) { desc = ctx.Params.Descriptor };
            try
            {
                Output(sb);
                errors = sb.GetErrors();
                //if (errors == null)
                //    sb.Commit();
            }
            finally
            {
                sb.Dispose();
            }
            if (errors != null) throw errors.GetException();
        }

        public virtual SqlBind GetKeys() { return null; }

        protected virtual Sql.Builder Output(Sql.Builder sb)
        {
            string s = SQL;
            char quot = sb.Context.GetMarker(false);
            SqlColumn scds = null;
            int bp = 0, cp = 1;
            while (cp < s.Length)
            {
                char ch = s[cp];
                switch (ch)
                {
                    case '#':
                        sb.Append(s, bp, cp - 1);
                        for (ch = s[cp - 1], bp = ++cp; cp < s.Length; cp++) if (s[cp] == '#') break;
                        if (cp == s.Length) throw new NotSupportedException("incomplete markup section");
                        int np = bp; while (s[np] <= ' ') np++;
                        //bp = ++cp;
                        int ep = np, xp = cp;
                        while (ep < cp) if (s[ep] != ':') ep++; else { xp = ep + 1; break; }
                        while (s[xp] <= ' ') xp++;
                        while (ep > np && s[ep - 1] <= ' ') ep--;
                        string pname = ep > np ? s.Substring(np, ep - np) : null;
                        ep = cp; while (ep > np && s[ep - 1] <= ' ') ep--;
                        string opts = ep > xp ? s.Substring(xp, ep - xp) : null;
                        if (scds == null) scds = new SqlColumn();
                        scds.Parse(opts);
                        sb.AppendParam(pname, scds);
                        bp = ++cp; continue;
                    case '-': // singleline comment 
                        if (++cp == s.Length || s[cp] != '-') break;
                        while (++cp < s.Length)
                            if (s[cp] == '\n') { cp += 2; break; }
                        continue;
                    case '/': // muiltiline comment, may contain useful info, like hints 
                        if (++cp == s.Length || s[cp] != '*') break;
                        while (++cp < s.Length)
                            if (s[cp] == '*' && (cp + 1 < s.Length) && s[cp + 1] == '/') { cp += 2; break; }
                        continue;
                    default:
                        if (ch != quot) break;
                        while (++cp < s.Length && s[cp] != quot) ;
                        break;
                }
                cp++;
            }
            sb.Append(s, bp, cp);
            return sb;
        }
    }

    public class SqlQuery : SqlScript
    {
        protected SqlBind _cols, _keys;
        protected SqlExpression _where;
        protected string _table_name;

        public SqlBind Columns { get { return _cols; } }

        public SqlQuery() { }
        public override SqlBind GetKeys() { return _keys; }
    }

    public class SqlInsert : SqlQuery
    {
        protected override Sql.Builder Output(Sql.Builder sb)
        {
            sb.Append("insert into ", _table_name, " (");
            int pos = 0;
            for (SqlBind cl = _cols; cl != null; cl = cl.Next)
            {
                if (pos++ > 0) sb.Comma();
                sb.Append(cl.Column.Name);
            }
            sb.psingle = pos;
            //TODO!!! add const value fields.
            sb.Append(") values ");
            for (int i = 0; i < Math.Max(1, sb.Context.Boost); i++)
            {
                if (i > 0) sb.Comma();
                pos = 0;
                sb.Append('(');
                for (SqlBind cl = _cols; cl != null; cl = cl.Next)
                {
                    if (pos++ > 0) sb.Comma();
                    //-sb.AppendParam(cl);
                }
                sb.Append(')');
            }
            return sb;
        }
    }

    public class SqlUpdate : SqlQuery
    {
        protected string _update;

        protected override Sql.Builder Output(Sql.Builder sb)
        {
            sb.Append("update ", _table_name, " set ");
            int pos = 0;
            if (_update != null) sb.Append(_update);
            else
                for (SqlBind cl = _cols; cl != null; cl = cl.Next)
                {
                    if (pos++ > 0) sb.Comma();
                    sb.Append(cl.Column.Name).Append('=');
                    //-sb.AppendParam(cl);
                }
            if (_where == null) return sb;
            sb.Append(" where ");
            _where.Output(sb);
            return sb;
        }
    }

    public class SqlDelete : SqlQuery
    {
        protected override Sql.Builder Output(Sql.Builder sb)
        {
            sb.Append("delete from ", _table_name);
            if (_where == null) return sb;
            sb.Append(" where ");
            _where.Output(sb);
            return sb;
        }
    }

    public class SqlSelect : SqlQuery
    {
        protected string _suffix;
        public SqlSelect() { }

        protected override Sql.Builder Output(Sql.Builder sb)
        {
            sb.Append("select ");
            sb.SelectList(_cols).Append(" from ", _table_name);
            // generate query conditions, if any.
            if (_where != null)
            {
                sb.Append(" where ");
                _where.Output(sb);
            }
            // add order by/group by/etc items.
            if (!string.IsNullOrEmpty(_suffix)) sb.Append(_suffix);
            return sb;
        }

        public SqlSelect Where(SqlExpression expr)
        {
            if (_where == null) _where = expr;
            else throw new InvalidOperationException("WHERE is already defined");
            return this;
        }
    }

}