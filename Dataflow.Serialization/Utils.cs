using System;
using System.Collections.Generic;
using System.Text;

namespace Dataflow.Serialization
{
    public class EscapeChars
    {
        public readonly static EscapeChars Instance = new EscapeChars();
        public const int LastPosition = (int)'\\';
        public readonly byte[] Table;

        public EscapeChars()
        {
            var bt = Table = new byte[LastPosition + 1];
            bt['\b'] = (byte)'b'; bt['\f'] = (byte)'f';
            bt['\n'] = (byte)'n'; bt['\r'] = (byte)'r';
            bt['\t'] = (byte)'t'; bt['\\'] = (byte)'\\';
            bt['"'] = (byte)'"'; bt['/'] = (byte)'/';
        }
    }

    public static class Text
    {
        public const string
            ISODateFormat = "yyyy-MM-ddTHH:mm:ss.fffZ",
            Base16String = "0123456789ABCDEF",
            Base64String = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        public const string
            NotInitialized = "not all required fields in the message have been set.";

        static Text()
        {
            int i; byte[] bt;
            Singles = new string[128];
            for (i = 32; i < 128; i++) Singles[i] = ((char)i).ToString();
            Base64Encode = bt = new byte[64];
            i = 0; foreach (var c in Base64String) bt[i++] = (byte)c;
            Base64Decode = bt = new byte[128];
            i = 0; foreach (var c in Base64String) bt[c] = (byte)(++i);
            Base16Bytes = bt = new byte[16];
            i = 0; foreach (var c in Base16String) bt[i++] = (byte)c;
        }

        public static readonly string[] Singles;
        public static byte[] Base64Encode { get; private set; }
        public static byte[] Base64Decode { get; private set; }
        public static byte[] Base16Bytes { get; private set; }

        public static int ParseHex(string s, int bp, int ep)
        {
            var rt = 0;
            for (; bp < ep; bp++)
            {
                int i;
                if ((i = s[bp]) < '0') break;
                if (i <= '9') i -= '0';
                else
                {
                    if (i < 'a') i |= 32;
                    if (i > 'f' || i < 'a') return rt;
                    i = i - ('a' - 10);
                }
                rt = rt * 16 + i;
            }
            return rt;
        }

        public static string LowerFirst(string s)
        {
            if (string.IsNullOrEmpty(s)) return s;
            return char.IsLower(s[0]) ? s : char.ToLower(s[0]) + s.Substring(1);
        }

        public static string UpperFirst(string s)
        {
            if (string.IsNullOrEmpty(s)) return s;
            return char.IsUpper(s[0]) ? s : char.ToUpper(s[0]) + s.Substring(1);
        }

        public static string CamelName(string s)
        {
            if (string.IsNullOrEmpty(s)) return s;
            var sb = new StringBuilder();
            var up_next = true;
            foreach (var c in s)
                if (c != '_') 
                    if (!up_next) sb.Append(c);
                    else { sb.Append(char.ToUpper(c)); up_next = false; }
                else up_next = true;
            return sb.ToString();
        }

        public static char ToLower(char c)
        {
            if (c < 0x80)
                return (c > 'Z' || c < 'A') ? c : (char)(c | 0x20);
            else return char.ToLower(c);
        }

        public static int AsHex(byte[] bt, int pos)
        {
            var rt = 0;
            while (true)
            {
                var i = (int)bt[pos++];
                if (i < '0') return rt;
                if (i <= '9') i -= '0';
                else
                {
                    if (i < 'a') i |= 32;
                    if (i > 'f' || i < 'a') return rt;
                    i = i - ('a' - 10);
                }
                rt = rt * 16 + i;
            }
        }

        public static int AsInt(byte[] bt, int pos)
        {
            for (var rt = 0; ; )
            {
                var i = bt[pos++] - '0';
                if (i < 0 || i > 9) return rt;
                rt = rt * 10 + i;
            }
        }

        public static string AsLine(byte[] bt, int pos)
        {
            for (var cp = pos; cp < bt.Length; cp++)
                if (bt[cp] == 13)
                    if (cp > pos) return Encoding.UTF8.GetString(bt, pos, cp - pos);
                    else break;
            return null;
        }

        public static bool CompareEqual(byte[] bt, int pos, string s)
        {
            foreach (var ch in s)
                if (bt[pos++] != (byte)ch) return false;
            return true;
        }

        public static int CompareHeader(byte[] bt, int pos, string s)
        {
            foreach (var ch in s)
                if ((bt[pos++] | 32) != (byte)ch) return 0;
            while (bt[pos] == ':' || bt[pos] == ' ') pos++;
            return pos;
        }
    }
}

namespace Dataflow.Utils
{
    public struct Fnv32Hash
    {
        private const uint principal = 0x811C9DC5, fnv32_init = 0x1000193;
        private uint _value;

        public uint Value { get { return _value; } }

        public Fnv32Hash(int first)
        {
            _value = (fnv32_init ^ (uint)first) * principal;
        }

        public void Reset()
        {
            _value = fnv32_init;
        }

        public uint Add(int vc)
        {
            return _value = (_value ^ (uint)vc) * principal;
        }

        public static uint Get(byte[] s)
        {
            uint hv = fnv32_init;
            foreach (var c in s)
                hv = (hv ^ c) * principal;
            return hv;
        }

        public static uint Get(string s)
        {
            uint hv = fnv32_init;
            foreach (var c in s)
                hv = (hv ^ c) * principal;
            return hv;
        }

        public static uint Get(string s, int prefix)
        {
            uint hv = (fnv32_init ^ (uint)prefix) * principal;
            if (s != null)
                foreach (var c in s)
                    hv = (hv ^ c) * principal;
            return hv;
        }
    }

    public class QuickHashItem<T>
    {
        public string Key { get; private set; }
        public uint Hash { get; private set; }
        public T Value { get; private set; }
        public QuickHashItem<T> Next;
        public QuickHashItem(T value, string key, int prefix = 0)
        {
            Value = value;
            Key = key;
            Hash = Fnv32Hash.Get(key, prefix);
        }
    }

    public struct QuickHash<T>
    {
        private QuickHashItem<T>[] _buckets;
        private int _count, _mask;

        public QuickHash(int init = 8)
        {
            _buckets = new QuickHashItem<T>[init];
            _mask = init - 1;
            if ((_mask & init) != 0) throw new ArgumentException();
            _count = 0;
        }

        public void Add(T value, string key, int prefix = 0)
        {
            Insert(new QuickHashItem<T>(value, key, prefix));
        }

        private void Insert(QuickHashItem<T> item)
        {
            int pos = (int)item.Hash & _mask;
            var ls = _buckets[pos];
            if (ls != null)
            {
                var i = _buckets.Length;
                if (i <= _count * 2)
                {
                    var os = _buckets;
                    _mask = i * 2 - 1;
                    _buckets = new QuickHashItem<T>[_mask + 1];
                    foreach (var bs in os)
                        for (var bx = bs; bx != null; )
                        {
                            var nx = bx.Next;
                            bx.Next = null;
                            Insert(bx);
                            bx = nx;
                        }
                    pos = (int)item.Hash & _mask;
                    ls = _buckets[pos];
                }
                item.Next = ls;
            }
            _buckets[pos] = item;
            _count++;
        }

        public QuickHashItem<T> Get(uint hash)
        {
            return _buckets[(int)hash & _mask];
        }
    }
}
