using System;
using Dataflow.Serialization;
using Dataflow.Remoting;

namespace Dataflow.Memcached
{
    public enum Opcode : uint
    {
        Get = 0, Set = 1, Add = 2, Replace = 3, Delete = 4, Inc = 5, Dec = 6, Quit = 7,
        Flush = 8, GetQ = 9, Noop = 0xA, Version = 0xB, GetK = 0xC, GetKq = 0xD,
        Append = 0xE, Prepend = 0xF, Stat = 0x10, SetQ = 0x11, AddQ = 0x12, ReplaceQ = 0x13,
        DeleteQ = 0x14, IncQ = 0x15, DecQ = 0x16, QuitQ = 0x17, FlushQ = 0x18,
        AppendQ = 0x19, PrependQ = 0x1A, Touch = 0x1C, Gat = 0x1D, GatQ = 0x1E,
        GatK = 0x23, GatKq = 0x24, Unused = 0xFF
    }

    public enum Status : uint
    {
        Success = 0, KeyNotFound = 1, KeyExists = 2, ValueTooBig = 3, InvalidArgs = 4,
        NotStored = 5, NotNumeric = 6, StillExecuting = 0x7F, UnknownCommand = 0x81, 
        OutOfMemory = 0x82
    }

    internal class ClientProtocol : ChannelProtocol
    {
        private const int iPacketHeaderSize = 24;
        private readonly PacketHeader _packet;
        private readonly StorageWriter _output;
        private readonly StorageReader _source;
        private bool _parsingBoby;

        internal static BitSet64 GetOps = new BitSet64((int)Opcode.Get, (int)Opcode.GetQ, (int)Opcode.GetK, (int)Opcode.GetKq);
        internal static Opcode[] QuietMap = new[] 
        { 
            Opcode.GetQ, Opcode.SetQ, Opcode.AddQ, Opcode.ReplaceQ, Opcode.DeleteQ, 
            Opcode.IncQ, Opcode.DecQ, Opcode.Unused/*Quit*/, Opcode.FlushQ, Opcode.GetQ, 
            Opcode.Unused /*Noop*/, Opcode.Unused/*Version*/, Opcode.GetKq, Opcode.GetKq, 
            Opcode.AppendQ, Opcode.PrependQ, Opcode.Unused/*Stat*/, Opcode.SetQ, 
            Opcode.AddQ, Opcode.ReplaceQ, Opcode.DeleteQ, Opcode.IncQ, Opcode.DecQ,
            Opcode.Unused/*QuitQ*/, Opcode.FlushQ, Opcode.AppendQ, Opcode.PrependQ,
            Opcode.Unused/*Touch*/, Opcode.GatQ, Opcode.GatQ, Opcode.GatKq, Opcode.GatKq
        };

        private class PacketHeader
        {
            private readonly byte[] _data;
            public PacketHeader() { _data = new byte[24]; }
            public void Reset(int opc)
            {
                _data[0] = 0x80;
                _data[1] = (byte)opc;
                for (var i = 2; i < _data.Length; i++) _data[i] = 0;
            }

            public byte[] Data { get { return _data; } }
            public uint Command { get { return _data[1]; } set { _data[1] = (byte)value; } }
            public uint DataType { get { return _data[5]; } set { _data[5] = (byte)value; } }
            public int StatusCode { get { return _data[7]; } }
            public uint ExtLength { get { return _data[4]; } set { _data[4] = (byte)value; } }
            public uint KeyLength
            {
                get { return _data[3] + ((uint)_data[2] << 8); }
                set
                {
                    var i = value;
                    _data[3] = (byte)i;
                    if (i < 256) return;
                    if (i > 0xFFFF) throw new ArgumentOutOfRangeException();
                    _data[2] = (byte)(i >> 8);
                }
            }
            public uint Opaque
            {
                get { return ((uint)_data[14] << 8) + ((uint)_data[13] << 16) + ((uint)_data[12] << 16) + _data[15]; }
                set
                {
                    var i = value;
                    _data[15] = (byte)i;
                    _data[14] = (byte)(i >>= 8);
                    _data[13] = (byte)(i >>= 8);
                    _data[12] = (byte)(i >> 8);
                }
            }
            public uint Reserved
            {
                get { return _data[7] + ((uint)_data[6] << 8); }
                set
                {
                    _data[7] = (byte)value;
                    if (value > 256) _data[6] = (byte)(value >> 8);
                }
            }
            public uint TotalBody
            {
                get { return ((uint)_data[10] << 8) + ((uint)_data[9] << 16) + _data[11]; }
                set
                {
                    var i = value;
                    _data[11] = (byte)i;
                    if (i < 0x10) return;
                    _data[10] = (byte)(i >>= 8);
                    if (i < 0x10) return;
                    _data[9] = (byte)(i >> 8);
                    // memcached data element size is limited to 1MB, so 4th byte should not be used.
                }
            }
            public ulong Cas
            {
                get
                {
                    ulong cas = _data[16];
                    for (var i = 17; i < 24; i++) cas = (cas << 8) + _data[i];
                    return cas;
                }
                set { for (var i = 23; value != 0 && i > 15; value = value >> 8, i--) _data[i] = (byte)value; }
            }
            public void WriteTo(StorageWriter output) { output.WriteBytes(_data, 0, _data.Length); }
        }

        internal ClientProtocol(Connection dtc) : base(dtc)
        {
            _packet = new PacketHeader();
            _output = new StorageWriter(null);
            _source = new StorageReader(null);
        }

        public override void CreateRequest(Request[] batch)
        {
            if (batch == null || batch.Length == 0)
                return;

            Batch = batch;
            _output.Reset(Connection.Data);

            // memcached supports request pipelining using "quiet" command opcodes and opaque-id to match results back.
            int opaque = 0;
            foreach (var request in batch)
            {
                request.Id = (++opaque == batch.Length) ? 0 : opaque;
                Encode(request.Params as CachedRequest, request.Id);
            }

            // sync internal writer state to data storage, and reset response parser state.
            _parsingBoby = false;
            _output.Flush();
        }

        private void Encode(CachedRequest request, int opaque)
        {
            if (request == null) 
                throw new ArgumentNullException();
            var opc = opaque == 0 ? request.Opcode : (int)QuietMap[request.Opcode];
            _packet.Reset(opc);
            if (opaque != 0) _packet.Opaque = (uint)opaque;
            var key = request.Key;
            switch ((Opcode)request.Opcode)
            {
                case Opcode.Get:
                case Opcode.GetQ:
                case Opcode.GetK:
                case Opcode.GetKq:
                case Opcode.Delete:
                case Opcode.DeleteQ:
                    _packet.KeyLength = _packet.TotalBody = (uint)key.Length;
                    _packet.WriteTo(_output);
                    _output.WriteBytes(key, 0, key.Length);
                    break;
                case Opcode.Set:
                case Opcode.SetQ:
                case Opcode.Add:
                case Opcode.AddQ:
                case Opcode.Replace:
                case Opcode.ReplaceQ:
                    _packet.KeyLength = (uint)key.Length;
                    _packet.ExtLength = 8;
                    var data = request.Data;
                    var vsz = data != null ? data.Length : 0;
                    _packet.TotalBody = (uint)(key.Length + vsz + 8);
                    if (request.Cas != 0) _packet.Cas = request.Cas;
                    _packet.WriteTo(_output);
                    _output.WriteB32BE((uint)request.Flags);
                    _output.WriteB32BE(request.Expires);
                    _output.WriteBytes(key, 0, key.Length);
                    if (vsz > 0) _output.WriteBytes(data, 0, vsz);
                    break;
                case Opcode.Noop:
                case Opcode.Version:
                case Opcode.Quit:
                case Opcode.QuitQ:
                    _packet.WriteTo(_output);
                    break;
                case Opcode.Flush:
                case Opcode.FlushQ:
                    var expires = request.HasExpires;
                    if (expires) _packet.ExtLength = _packet.TotalBody = 4;
                    _packet.WriteTo(_output);
                    if (expires) _output.WriteB32BE(request.Expires);
                    break;
                case Opcode.Inc:
                case Opcode.IncQ:
                case Opcode.Dec:
                case Opcode.DecQ:
                    _packet.ExtLength = 20;
                    _packet.KeyLength = (uint)key.Length;
                    _packet.TotalBody = (uint)(20 + key.Length);
                    if (request.HasCas) _packet.Cas = request.Cas;
                    _packet.WriteTo(_output);
                    _output.WriteB64BE(request.Delta);
                    _output.WriteB64BE(request.Flags);
                    _output.WriteB32BE(request.Expires);
                    _output.WriteBytes(key, 0, key.Length);
                    break;
                case Opcode.Append:
                case Opcode.AppendQ:
                case Opcode.Prepend:
                case Opcode.PrependQ:
                    _packet.KeyLength = (uint)key.Length;
                    var pdata = request.Data;
                    _packet.TotalBody = (uint)(key.Length + pdata.Length);
                    if (request.Cas != 0) _packet.Cas = request.Cas;
                    _packet.WriteTo(_output);
                    _output.WriteBytes(key, 0, key.Length);
                    _output.WriteBytes(pdata, 0, pdata.Length);
                    break;
                default:
                    throw new NotSupportedException();
            }
        }
        
        private byte[] ExtractBytes(int sz) 
        { 
            return sz > 0 ? _source.GetBytes(new byte[sz], ref sz) : null; 
        }

        private void Decode(CachedResponse rsp)
        {
            var cas = _packet.Cas;
            if (cas != 0) rsp.Cas = cas;
            switch ((Opcode)_packet.Command)
            {
                case Opcode.Get:
                case Opcode.GetQ:
                case Opcode.GetK:
                case Opcode.GetKq:
                    rsp.Flags = _source.GetB32BE();
                    var szk = _packet.KeyLength;
                    // skip key bytes, returned when GetK was issued.
                    if (szk != 0) _source.Skip((int)szk);
                    szk = _packet.TotalBody - szk - 4;
                    rsp.Data = ExtractBytes((int)szk);
                    break;
                case Opcode.Set:
                case Opcode.SetQ:
                case Opcode.Add:
                case Opcode.AddQ:
                case Opcode.Replace:
                case Opcode.ReplaceQ:
                    break;
                case Opcode.Inc:
                case Opcode.IncQ:
                case Opcode.Dec:
                case Opcode.DecQ:
                    rsp.Counter = _source.GetB64BE();
                    break;
                case Opcode.Noop:
                case Opcode.Delete:
                case Opcode.DeleteQ:
                case Opcode.Flush:
                    break;
                case Opcode.Version:
                case Opcode.Stat:
                    rsp.Data = ExtractBytes((int)_packet.TotalBody);
                    break;
                case Opcode.Append:
                case Opcode.AppendQ:
                case Opcode.Prepend:
                case Opcode.PrependQ:
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        public override bool ParseResponse()
        {
            _source.Reset(Connection.Data);
            do
            {
                // get full header before collecting body parts.
                if (_parsingBoby == false)
                {
                    if (!_source.IsAvailable(1))
                        break;
                    var rc = _source.PeekByte();
                    if (rc != 0x81)
                        throw new ProtocolException("response magic#");
                    if (!_source.IsAvailable(iPacketHeaderSize))
                        break;
                    var hsz = iPacketHeaderSize;
                    _source.GetBytes(_packet.Data, ref hsz);
                    _parsingBoby = true;
                }

                // verify that body data is completely in our storage.
                var bodySize = (int)_packet.TotalBody;
                if (bodySize > 0 && !_source.IsAvailable(bodySize))
                    break;

                // match request instance from the batch using opaque-id.
                var id = (int)_packet.Opaque;
                if (id < 0 || id > Batch.Length)
                    throw new ProtocolException("opaque");
                var pos = (id != 0 ? id : Batch.Length) - 1;
                var request = Batch[pos];
                if(request == null)
                    throw new ProtocolException("dup-id");
                Batch[pos] = null;

                // validate response instance type and decode body bytes.
                var response = request.Result as CachedResponse;
                if (response == null)
                {
                    _source.Skip(bodySize);
                    request.Fail(new Signal(Signal.iBadLpc));
                }
                else
                {
                    response.Status = _packet.StatusCode;
                    if (response.Status == (int)Status.Success)
                        Decode(response);
                    else _source.Skip(bodySize);
                }
                _parsingBoby = false;

                // done with reading reasponse stream.
                if (id == 0)
                {
                    if (_source.IsAvailable(1))
                        throw new ProtocolException("no more data expected");
                    // set result statuses for quiet responses.
                    foreach (var quiet in Batch)
                    {
                        if (quiet == null) continue;
                        (quiet.Result as CachedResponse).Status = GetOps.Has((request.Params as CachedRequest).Opcode) ? (int)Status.KeyNotFound : (int)Status.Success;
                    }
                    return true;
                }
            } while (true);

            // update source position to connection.
            _source.SyncToStorage();
            return false;
        }
    }
}