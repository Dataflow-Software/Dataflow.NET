/* 
 * Key-Value memcache for .NET, using virtual(non-GC) memory.
 * Supports in-process api and binary memcached protocols over tcp.
 * Optimized for multi-get/pipelined access profile.
 * (c) Viktor Poteryakhin, 2013-2015.
 */

using System;
using System.Diagnostics;
using Dataflow.Memcached;

namespace Dataflow.Caching
{
    public partial class LocalCache
    {
        public struct GlobalStats
        {
            private ulong _lastRequests, _lastBytesIn, _lastBytesOut;
            public int CurConns, TotalConns;

            // public access properties.
            public uint ProcessId { get; private set; }
            public ulong CurrentItems { get; internal set; } //--
            public ulong CurrentBytes { get; internal set; } //--
            public ulong MemoryLimit { get; private set; } //--
            public ulong Evictions { get; internal set; } //--
            public ulong CountUpdates { get; internal set; } //--
            public ulong CountGetHits { get; internal set; } //--
            public ulong CountMisses { get; internal set; } //--
            public ulong RequestCount { get; internal set; } //--

            public ulong BytesIn { get; internal set; } //--
            public ulong BytesOut { get; internal set; } //--

            public ulong Rps { get; internal set; } //--
            public ulong BpsIn { get; internal set; } //--
            public ulong BpsOut { get; internal set; } //--

            public uint Uptime { get; internal set; } //--

            public void Init(uint mbLimit)
            {
                ProcessId = (uint)Process.GetCurrentProcess().Id;
                if (mbLimit > 512000) throw new ArgumentOutOfRangeException("mem.limit(mb):" + mbLimit);
                MemoryLimit = mbLimit * 1024 * 1024;
            }

            public void Flush()
            {
                CountGetHits = CountUpdates = Evictions = 0;
                CountMisses = CurrentBytes = CurrentItems = 0;
                RequestCount = 0;
                Rps = BpsIn = BpsOut = 0;
            }

            public void CopyTo(CachedStats cs)
            {
                cs.Uptime = Uptime;
                cs.CurrentItems = CurrentItems;
                cs.CurrentBytes = CurrentBytes;
                cs.TotalMemory = MemoryLimit;
                cs.Evictions = Evictions;
                cs.Requests = RequestCount;
                cs.GetHits = CountGetHits;
                cs.Updates = CountUpdates;
                cs.Misses = CountMisses;
                cs.BytesOut = BytesOut;
                cs.BytesIn = BytesIn;
                cs.BpsOut = BpsOut;
                cs.BpsIn = BpsIn;
                cs.Rps = Rps;
            }

            public void UpdateStatistics(long clock, uint elapsed)
            {
                Uptime = (uint)clock;
                BpsIn = 1000 * (BytesIn - _lastBytesIn) / elapsed;
                _lastBytesIn = BytesIn;
                BpsOut = 1000 * (BytesOut - _lastBytesOut) / elapsed;
                _lastBytesOut = BytesOut;
                Rps = 1000 * (RequestCount - _lastRequests) / elapsed;
                _lastRequests = RequestCount;
            }
        }
    }
}
