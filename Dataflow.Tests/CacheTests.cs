using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;
using Dataflow.Memcached;
using Dataflow.Caching;
using Dataflow.Core;
using Dataflow.Remoting;

namespace Dataflow.Tests {

    //[TestFixture( "tcp://192.168.1.5:11213", true, Category = "net.cached", Description = "tcp/ip connection tests" )]
    //[TestFixture( "pipe://memcached", true, Category = "net.cached", Description = "named pipe(ipc) connection tests" )]
    //[TestFixture( 32, Category = "local.cache", Description = "LocalCache based tests" )]
    public class NCachedTests {
        private const string sCommonKey = "shared-test-key";
        //private readonly Memcached.Service _service;
        private readonly IChannelAsync _target;
        private CachedClient _client;
        private byte[] _foo;

        public NCachedTests( string target, bool startNew ) {
            //if( startNew ) {
            //    var config = new Memcached.CacheSettings( 64 ) { DataUri = new WebRequestUri( target ) };
            //    _service = new Memcached.Service( config );
            //    _service.Start();
            //}
            //_target = new RemoteQueue( target, DataEncoding.Memcached, 1, 1 );
        }

        public NCachedTests( int memSize ) {
            _target = new LocalCache( new CachedConfiguration { CacheSize = 32 } );
        }

        public static void RuntimeExecute( string s, bool local = true ) {
            int mb = 0;
            var test = int.TryParse( s, out mb ) ? new NCachedTests( mb ) : new NCachedTests( s, local );
            test.RuntimeExecute();
            Console.WriteLine( "Net.Cached: passed for {0} target", mb != 0 ? "local[" + s + "]" : s );
        }

        public void RuntimeExecute() {
            SetUp();
            TestSimpleAndBatchedCalls();
            BatchGetTest( 10 );
            BatchGetTest( 100 );
            BatchGetTest( 1000 );
            GetSetCachedValue( "abc", "data-as-data" );
            GetSetCachedValue( "quite-long-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "no-so-long-as-key-data-000000000000000000000" );
            IncrementDecrementTest( 100, 25 );
            IncrementDecrementTest( 200, -125 );
            IncrementDecrementTest( 300, -425 );
            IncrementDecrementTest( 345634600, -5632425 );
            IncrementDecrementTest( 345634600, 95632425 );
            CasOperationsTest();
            AppendWithCasTest();
            PrependWithCasTest();
            PrependBigDataTest();
            DeleteKeyTest();
            SetNullValueTest();
            SetBigDataTest();
            KeyExpirationTest();
            AddReplaceKeyTest();
        }

        [SetUp]
        public void SetUp() {
            _client = new CachedClient( _target );
            _foo = GetBytes( "foo" );
        }

        [TearDown]
        public void TearDown() {

        }

        public byte[] GetBytes( string data ) {
            return Encoding.UTF8.GetBytes( data );
        }

        protected CachedClient GetClient() {
            if( _client == null ) throw new ArgumentNullException( "client" );
            _client.Flush( 0 );
            return _client;
        }

        [TestCase( "abc", "data-for-abc" )]
        [TestCase( "longer-then-usual-key-value-just-to-test-something-abcdefghijklmnopqrstuvwxyz.", "data" )]
        public void GetSetCachedValue( string key, string value ) {
            var client = GetClient();
            var data = GetBytes( value );
            client.Set( key, data );
            Assert.That( client.Get( key ).SequenceEqual( data ) );
        }

        [TestCase( 100, 25, Description = "Increment test" )]
        [TestCase( 200, -125, Description = "Decrement test" )]
        [TestCase( 300, -425, Description = "Decrement below 0" )]
        public void IncrementDecrementTest( long defval, long change ) {
            var client = GetClient();
            if( change > 0 ) {
                Assert.AreEqual( defval, client.Inc( sCommonKey, defval + 5, defval ) );
                Assert.AreEqual( defval + change, client.Inc( sCommonKey, change, -1 ) );
            }
            else {
                Assert.AreEqual( defval, client.Dec( sCommonKey, defval + 5, defval ) );
                defval += change;
                change = -change;
                Assert.AreEqual( defval < 0 ? 0 : defval, client.Dec( sCommonKey, change, -1 ) );
            }
            long lval = 0;
            var s = client.GetText( sCommonKey );
            Assert.That( long.TryParse( s, out lval ), "correct long string value" );
        }

        [TestCase]
        public virtual void CasOperationsTest() {
            var client = GetClient();
            var s1 = client.Set( sCommonKey, _foo );
            Assert.AreEqual( s1.Status, 0, "Set failed." );
            Assert.AreNotEqual( s1.Cas, 0, "No cas value was returned." );

            var g2 = client.GetCas( sCommonKey );

            Assert.That( g2.Data.SequenceEqual( _foo ), "Expected 'foo'" );
            Assert.AreEqual( g2.Cas, s1.Cas, "Cas values should match." );

            var s3 = client.Set( sCommonKey, GetBytes( "bar" ), 0, s1.Cas + 1 );
            Assert.AreNotEqual( s3.Status, 0, "Cas mismatch should fail." );
            var baz = GetBytes( "baz" );
            var s4 = client.Set( sCommonKey, baz, 0, g2.Cas );
            Assert.AreEqual( s4.Status, 0, "Matching Cas should succeed." );

            var g5 = client.GetCas( sCommonKey );
            Assert.That( g5.Data.SequenceEqual( baz ), "Expected 'baz'." );
        }

        [TestCase]
        public void AppendWithCasTest() {
            var client = GetClient();
            var s1 = client.Set( sCommonKey, _foo );

            Assert.AreEqual( s1.Status, 0, "Set failed." );
            Assert.AreNotEqual( s1.Cas, 0, "Cas value was not returned." );

            var a2 = client.Append( sCommonKey, new[] { (byte)'l' }, s1.Cas );
            Assert.AreEqual( a2.Status, 0, "Append must succeed." );

            var g3 = client.GetCas( sCommonKey );
            Assert.That( g3.Data.SequenceEqual( GetBytes( "fool" ) ), "Expected 'fool'" );
            Assert.AreEqual( a2.Cas, g3.Cas, "Cas values must match." );

            var a4 = client.Append( sCommonKey, _foo, s1.Cas );
            Assert.AreNotEqual( a4.Status, 0, "CAS mismatch should fail" );
        }

        [TestCase]
        public void PrependWithCasTest() {
            var client = GetClient();
            var s1 = client.Set( sCommonKey, GetBytes( "ool" ) );
            Assert.AreEqual( s1.Status, 0, "Set failed" );
            Assert.AreNotEqual( s1.Cas, 0, "Cas value Not set" );
            var p2 = client.Prepend( sCommonKey, new[] { (byte)'f' }, s1.Cas );
            Assert.AreEqual( p2.Status, 0, "Prepend should work" );
            var g3 = client.GetCas( sCommonKey );
            Assert.That( g3.Data.SequenceEqual( GetBytes( "fool" ) ), "Expected 'fool'" );
            Assert.AreEqual( p2.Cas, g3.Cas, "Cas values must match." );
            var p4 = client.Prepend( sCommonKey, _foo, s1.Cas );
            Assert.AreNotEqual( p4.Status, 0, "Prepend should fail" );
        }

        [TestCase]
        public void PrependBigDataTest() {
            var client = GetClient();

            byte[] b1 = new byte[4096], b2 = new byte[8192];
            for( var i = 0; i < b1.Length; i++ ) b1[i] = (byte)'a';
            for( var i = 0; i < b2.Length; i++ ) b2[i] = (byte)'b';
            var s1 = client.Set( "CasPrependBig", b1 );
            Assert.AreEqual( s1.Status, 0, "Set failed" );
            Assert.AreNotEqual( s1.Cas, 0, "Cas value" );
            var p2 = client.Prepend( "CasPrependBig", b2, s1.Cas );
            Assert.AreEqual( p2.Status, 0, "Prepend operation" );
            var g3 = client.GetCas( "CasPrependBig" );
            Assert.AreNotEqual( g3.Data, null );
            Assert.AreEqual( g3.Data.Length, b1.Length + b2.Length, "Invalid data size" );
            Assert.AreEqual( p2.Cas, g3.Cas, "Cas values must match" );
            var pos = 0;
            foreach( var c in g3.Data ) {
                Assert.AreEqual( c, (pos < b2.Length ? b2[pos] : b1[pos - b2.Length]), "Prepend: data corrupted" );
                pos++;
            }
        }

        [TestCase]
        public void DeleteKeyTest() {
            var client = GetClient();
            Assert.AreEqual( client.Set( sCommonKey, _foo ).Status, 0, "Set failed" );
            Assert.AreEqual( client.Delete( sCommonKey ).Status, 0, "Delete failed" );
            Assert.AreNotEqual( client.GetCas( sCommonKey ).Status, 0, "Remove failed" );
        }

        [TestCase]
        public void SetNullValueTest() {
            var client = GetClient();
            Assert.AreEqual( client.Set( "NullValue", null ).Status, 0, "Set failed" );
            var g1 = client.GetCas( "NullValue" );
            Assert.AreEqual( g1.Status, 0, "Get-null failed" );
            Assert.IsNull( g1.Data, "Result should be null" );
        }

        [TestCase( 100 )]
        public virtual void BatchGetTest( int count ) {
            /*
            var prefix = Environment.TickCount + "$";
            var client = GetClient();
            //var queue = client.Target as RemoteQueue;
            try {
                //if( queue != null ) queue.Pipeline = count;
                var keys = new List<string>( count );
                // set multiple values into cache
                var sbatch = new CachedClient.Batch();
                for( var i = 0; i < count; i++ ) {
                    var k = prefix + "-Batch(Gets)-" + i;
                    keys.Add( k );
                    sbatch.Set( k, BitConverter.GetBytes( i ) );
                }
                var clock = new System.Diagnostics.Stopwatch();
                clock.Restart();
                sbatch.Execute( client );
                clock.Stop();
                Console.WriteLine( "-- batch set: elapsed time = {0} ms, count = {1}", clock.ElapsedMilliseconds, count );
                // get multiple values from cache
                var gbatch = new CachedClient.Batch();
                foreach( var key in keys )
                    gbatch.Get( key );
                clock.Restart();
                gbatch.Execute( client );
                clock.Stop();
                Console.WriteLine( "-- batch get: elapsed time = {0} ms, count = {1}", clock.ElapsedMilliseconds, count );

                var pos = 0;
                foreach( var r in gbatch.Responses )
                    Assert.That( r.Status == 0 && BitConverter.ToInt32( r.Data, 0 ) == pos++, "check for get misses" );
            }
            finally {
                //if( queue != null ) queue.Pipeline = 0;
            }
 */ 
        }

        /*
                [TestCase]
                public void FlushTest()
                {
                    using (MemcachedClient client = GetClient())
                    {
                        Assert.IsTrue(client.Store(StoreMode.Set, "qwer", "1"), "Initialization failed");
                        Assert.IsTrue(client.Store(StoreMode.Set, "tyui", "1"), "Initialization failed");
                        Assert.IsTrue(client.Store(StoreMode.Set, "polk", "1"), "Initialization failed");
                        Assert.IsTrue(client.Store(StoreMode.Set, "mnbv", "1"), "Initialization failed");
                        Assert.IsTrue(client.Store(StoreMode.Set, "zxcv", "1"), "Initialization failed");
                        Assert.IsTrue(client.Store(StoreMode.Set, "gfsd", "1"), "Initialization failed");

                        Assert.AreEqual("1", client.Get("mnbv"), "Setup for FlushAll() failed");

                        client.FlushAll();

                        Assert.IsNull(client.Get("qwer"), "FlushAll() failed.");
                        Assert.IsNull(client.Get("tyui"), "FlushAll() failed.");
                        Assert.IsNull(client.Get("polk"), "FlushAll() failed.");
                        Assert.IsNull(client.Get("mnbv"), "FlushAll() failed.");
                        Assert.IsNull(client.Get("zxcv"), "FlushAll() failed.");
                        Assert.IsNull(client.Get("gfsd"), "FlushAll() failed.");
                    }
                }
  
          [TestCase]
                public void StoreLongTest() {
                    var client = GetClient();
                    Assert.IsTrue( client.Store( StoreMode.Set, "TestLong", 65432123456L ), "StoreLong failed." );
                    Assert.AreEqual( 65432123456L, client.Get<long>( "TestLong" ) );
                }
        */
        [TestCase]
        public void SetBigDataTest() {
            var data = new byte[225 * 1024];
            for( var i = 0; i < data.Length; i++ )
                data[i] = (byte)i;

            var client = GetClient();
            Assert.AreEqual( client.Set( sCommonKey, data ).Status, 0, "Set failed" );

            var getd = client.Get( sCommonKey );
            for( var i = 0; i < data.Length; i++ )
                Assert.AreEqual( data[i], getd[i], "Set data are corrupt" );
        }

        [TestCase]
        public void KeyExpirationTest() {
            var client = GetClient();
            Assert.AreEqual( client.Set( "expire$1", _foo, 1 ).Status, 0, "Set failed" );
            Assert.AreEqual( client.GetCas( "expire$1" ).Status, 0, "Get failed" );
            System.Threading.Thread.Sleep( 2500 );
            Assert.AreNotEqual( client.GetCas( "expire$1" ).Status, 0, "Not expired" );
        }

        [TestCase]
        public void AddReplaceKeyTest() {
            var client = GetClient();
            Assert.AreEqual( client.Set( sCommonKey, GetBytes( "set" ) ).Status, 0, "Set failed" );
            Assert.AreEqual( "set", client.GetText( sCommonKey ), "Get/Set failed" );
            Assert.AreNotEqual( client.Add( sCommonKey, _foo ).Status, 0, "Add should fail" );
            Assert.AreEqual( "set", client.GetText( sCommonKey ), "Add succeded" );
            Assert.AreEqual( client.Replace( sCommonKey, GetBytes( "replace-ok" ) ).Status, 0, "Replace failed" );
            Assert.AreEqual( "replace-ok", client.GetText( sCommonKey ), "Value not replaced" );
            Assert.AreEqual( client.Delete( sCommonKey ).Status, 0, "Delete failed" );
            Assert.AreNotEqual( client.Replace( sCommonKey, _foo ).Status, 0, "Replace must fail" );
            Assert.That( client.GetCas( sCommonKey ).Status != 0, "Replace sneaked past" );
            Assert.AreEqual( client.Add( sCommonKey, GetBytes( "add-ok" ) ).Status, 0, "Add failed" );
            Assert.AreEqual( "add-ok", client.GetText( sCommonKey ), "Add failed" );
        }

        [TestCase]
        public void TestSimpleAndBatchedCalls() {
            var client = GetClient();
            
            var s = client.Version();
            //var queue = client.Target as RemoteQueue;
            try {
                //if( queue != null ) queue.Pipeline = 100;

                Console.WriteLine( s );
                client.Set( "abc", "hello".AsBytes(), 30 );
                client.Prepend( "abc", " world".AsBytes() );
                s = client.GetText( "abc" );
                Assert.AreEqual( " worldhello", s, "set+prepend then get" );
                var lv = client.Inc( "count", 1, 5, 30 );
                var nv = client.Dec( "count", 1, 0, 30 );
                Assert.AreEqual( lv, nv + 1, "inc/dec commands chain" );
                Assert.That( client.GetCas( "zoo" ).Status != 0, "key was not set" );

                var batch = new CachedClient.BatchRequest();
                batch.Set( "abc", "hello".AsBytes(), 30 );
                batch.Get( "def" );
                batch.Get( "abc" );
                batch.Set( "abc", "ooops".AsBytes(), 30 );
                batch.Get( "abc" );
                batch.Delete( "abc" );
                batch.Get( "abc" );
                
                client.Execute(batch);
                Assert.IsTrue( batch.Result[6].Status == 1, "get after delete" );
                Assert.AreEqual( "ooops", batch.Result[4].Text, "get after change" );
            }
            finally {
                //if( queue != null ) queue.Pipeline = 0;
            }
        }
    }
}
