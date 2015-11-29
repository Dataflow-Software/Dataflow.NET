using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public abstract class Connection
    {
        private static AsyncCallback _readAsync = ReadAsyncEx;
        private static AsyncCallback _sendAsync = SendAsyncEx;

        protected AwaitIo _awaitable;
        protected Option _options;

        public DataStorage Data { get; private set; }
        public int ContentSize { get; set; }
        public Uri EndPoint { get; private set; }

        [Flags]
        public enum Option
        {
            KeepReading = 1, Dead = 2, Open = 4, Active = 8,
            AutoCommit = 16, AsyncExec = 32, Close = 64,
            KeepAlive = 128, SkipOpen = 256
        }
        private void SetFlags(bool value, Option flag) { if (value) _options |= flag; else _options &= ~flag; }

        public bool Connected { get { return _options.HasFlag(Option.Active); } set { SetFlags(value, Option.Active); } }
        public bool KeepAlive { get { return _options.HasFlag(Option.KeepAlive); } set { SetFlags(value, Option.KeepAlive); } }
        public bool KeepReading { get { return _options.HasFlag(Option.KeepReading); } set { SetFlags(value, Option.KeepReading); } }

        protected Connection(Uri ep = null)
        {
            EndPoint = ep;
            _awaitable = new AwaitIo(false);
            Data = new DataStorage();
        }

        protected void CompleteAsync(int count)
        {
            _awaitable.CompleteAsync(count);
        }

        protected void CompleteAsync(Exception ex)
        {
            _awaitable.CompleteAsync(ex);
        }

        protected virtual Stream GetDataStream()
        {
            return null;
        }

        protected bool CloseStream<T>(ref T stream) where T : Stream
        {
            var temp = stream;
            if (temp != null)
            {
                stream = null;
                //temp.Close();
            }
            return false;
        }

        public virtual AwaitIo AcceptAsync()
        {
            throw new NotSupportedException("accept");
        }

        public virtual  AwaitIo CloseAsync(bool panic = false)
        {
            Connected = false;
            var dts = Data;
            if (dts != null)
            {
                Data = null;
                dts.Dispose();
            }
            return AwaitIo.Done;
        }

        public virtual AwaitIo ConnectAsync()
        {
            Connected = true;
            return AwaitIo.Done;
        }

        public virtual AwaitIo FlushAsync(bool final)
        {
            if (Data.ContentSize == 0)
                return AwaitIo.Done;
            return SendAsync();
        }

        private static void ReadAsyncEx(IAsyncResult iar)
        {
            var connection = (Connection)iar.AsyncState;
            var stream = connection.GetDataStream();
            try
            {
                var bts = 0;// stream..EndRead(iar);
                //todo: should 0 bytes reads be treated as close/abort ??
                if (bts > 0) connection.Data.ReadCommit(bts);
                connection.CompleteAsync(bts);
            }
            catch (Exception fault)
            {
                connection.CompleteAsync(fault);
            }
        }

        public virtual AwaitIo ReadAsync()
        {
            _awaitable.Reset();
            var bts = Data.GetSegmentToRead();
            var stream = GetDataStream();
            //stream.BeginRead(bts.Buffer, bts.Offset + bts.Count, bts.Size - bts.Count, _readAsync, this);
            return _awaitable;
        }
        
        private static void SendAsyncEx(IAsyncResult iar)
        {
            var connection = (Connection)iar.AsyncState;
            var stream = connection.GetDataStream();
            try
            {
                //stream.EndWrite(iar);
                // when no more data is left to send, complete async call.
                if (connection.Data.CommitSentBytes())
                {
                    connection.CompleteAsync(0);
                    return;
                }
                // awaitable is already given to original SendAsync caller, so no resets.
                var ds = connection.Data.GetSegmentToSend();
                //stream.BeginWrite(ds.Buffer, ds.Offset, ds.Count, _sendAsync, connection);
            }
            catch (Exception fault)
            {
                connection.CompleteAsync(fault);
            }
        }

        public virtual AwaitIo SendAsync()
        {
            _awaitable.Reset();
            var stream = GetDataStream();
            var bts = Data.GetSegmentToSend();
            //stream.BeginWrite(bts.Buffer, bts.Offset, bts.Count, _sendAsync, this);
            return _awaitable;
        }
    }
}
