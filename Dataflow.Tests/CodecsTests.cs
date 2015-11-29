using System;
using System.IO;
using System.Diagnostics;
using NUnit.Framework;
using System.Runtime.Serialization.Json;
using Dataflow.Remoting;
using Dataflow.Serialization;
//using Dataflow.Caching;
using Newtonsoft.Json;

namespace Dataflow.Tests
{

    //[TestFixture( "tcp://192.168.1.5:8181", true, Category = "rpc.tcp", Description = "tcp/ip connection RPC tests")]
    //[TestFixture( "pipe://echorpc", true, Category = "rpc.pipe", Description = "named pipe(ipc) RPC tests" )]
    [TestFixture(Category = "rpc.local", Description = "local code RPC(LPC) tests")]
    public class Codecs_Tests
    {
        public delegate int IntLambda(int seed);
        public int Mode;
        public Codecs_Tests()
        {
        }

        public static void RuntimeExecute(int speedTest = 0)
        {
            var test = new Codecs_Tests();
            if (speedTest > 0)
            {
                GoogleCsharpComparison(speedTest);
                var clock = new Stopwatch();
                var tls = new IntLambda[] { 
                    i => test.SingleValuesProtoTests( i ), i => test.SingleValuesJsonTests( i ), 
                    //i => test.SingleValuesNewtonTests( i ), i => test.SingleValuesDcJsonTests( i ),
                    i => test.RepeatedValuesProtoTests( i & 63 ), i => test.RepeatedValuesJsonTests( i & 63 ), 
                    //i => test.RepeatedValuesNewtonTests( i & 63 ), i => test.RepeatedValuesDcJsonTests( i & 63 )
                };
                // warm-up
                foreach (var rt in tls)
                {
                    var bytes = 0;
                    for (int i = 0; i < 2; i++) bytes += rt(i);
                }
                // timed run
                for (test.Mode = 0; test.Mode < 2; test.Mode++)
                    foreach (var rt in tls)
                    {
                        var bytes = 0;
                        clock.Restart();
                        for (int i = 0; i < speedTest * 100; i++)
                            bytes += rt(i);
                        clock.Stop();
                        var adt = (clock.ElapsedMilliseconds != 0 ? clock.ElapsedMilliseconds : 1) * 100000;
                        Console.WriteLine("-- elapsed time = {0} ms, speed = {1} mb/s, avg size = {2}", clock.ElapsedMilliseconds, Math.Round((double)((long)bytes * speedTest) / adt, 2), bytes / (speedTest * 100));
                    }
            }
            test.RuntimeExecute();
            Console.WriteLine("Codecs: message encoding and decoding tests passed.");
        }

        public void RuntimeExecute()
        {
            SetUp();
            try
            {
                Mode = 0;
                SingleValuesProtoTests(-5);
                SingleValuesProtoTests(99);
                SingleValuesJsonTests(22);
                RepeatedValuesProtoTests(45);
                RepeatedValuesJsonTests(33);
                AggregatedMessageTests(785);
            }
            finally
            {
                TearDown();
            }
        }

        [SetUp]
        public void SetUp()
        {
        }

        [TearDown]
        public void TearDown()
        {
        }

        protected OptFields CreateSingle(int i)
        {
            var opts = new OptFields
            {
                I32 = 7 + i,
                I64 = -5 - i,
                Bln = true,
                Bts = new byte[3] { 0x97, (byte)i, 0x77 },
                Str = "hello world",
                Chr = 'Z',
                Si32 = -19,
                Si64 = -33,
                Ui32 = 42,
                Ui64 = 0x4FFFFFFFF4,
                Cur = new Currency(101.202 + i),
                Dat = new DateTime(2011 + i % 32, 9, 29, 11, 22, 33),
                Dbl = 98765.04321D - i,
                Flt = 12345.67F,
                Dec = new Decimal(-435.908),
                Enu = Colors.Green,
                F32 = 2789498423,
                F64 = 0xFFFFFFFFFFFFFFFF
            };
            return opts;
        }

        protected RepFields CreateRepeated(int i32)
        {
            var reps = new RepFields();
            for (int i = 0; i < i32; i++)
                reps.AddI32(100 + i);
            reps.AddI32(1); reps.AddI32(-45); reps.AddI32(102); reps.AddI32(-800);
            reps.AddI64(-708); reps.AddI64(456); reps.AddI64(0); reps.AddI64(-77);
            reps.AddBln(true); reps.AddBln(false); reps.AddBln(true);
            reps.AddBts(new byte[2] { 1, 2 }); reps.AddBts(new byte[5] { 11, 22, 33, 44, 55 });
            reps.AddStr("one"); reps.AddStr("two"); reps.AddStr("three");
            reps.AddChr('A'); reps.AddChr('b'); reps.AddChr('C'); reps.AddChr('D'); reps.AddChr('e');
            reps.AddSi32(-11); reps.AddSi32(32); reps.AddSi32(-987654321);
            reps.AddSi64(-223311); reps.AddSi64(42); reps.AddSi64(-9876543210098765);
            reps.AddUi32(11); reps.AddUi32(32); reps.AddUi32(987654321);
            reps.AddUi64(223311); reps.AddUi64(42); reps.AddUi64(9876543210098765);
            reps.AddCur(new Currency(1100.345)); reps.AddCur(new Currency(451100.3451));
            reps.AddDat(new DateTime(2011, 9, 29, 11, 22, 33)); reps.AddDat(new DateTime(2010, 10, 27, 12, 12, 12));
            reps.AddFlt(12345.67F); reps.AddFlt(12345.68F); reps.AddFlt(-12345.67F);
            reps.AddDbl(12345.67D); reps.AddDbl(12345.68D); reps.AddDbl(-12345.67D);
            reps.AddDec(new Decimal(-435.908)); reps.AddDec(new Decimal(123435.90804));
            reps.AddEnu(Colors.Red); reps.AddEnu(Colors.Blue); reps.AddEnu(Colors.Green);
            reps.AddF32(88); reps.AddF32(3790874215); reps.AddF32(1009876535); reps.AddF32(25); reps.AddF32(0);
            reps.AddF64(99223311); reps.AddF64(5848644545353376343); reps.AddF64(987654321009876535);
            return reps;
        }

        [TestCase(-1)]
        [TestCase(42)]
        public void SingleValuesProtoTestsN(int seed) { SingleValuesProtoTests(seed); }
        public int SingleValuesProtoTests(int seed)
        {
            var opts = CreateSingle(seed);
            var bto = opts.ToByteArray();
            if (Mode == 0) return bto.Length;
            var optz = new OptFields();
            optz.MergeFrom(bto);
            Assert.That(opts.Equals(optz), "PB single values encode-decode");
            return bto.Length;
        }

        [TestCase(-9)]
        [TestCase(75)]
        public void SingleValuesJsonTestsN(int seed) { SingleValuesJsonTests(seed); }
        public int SingleValuesJsonTests(int seed)
        {
            var opts = CreateSingle(seed);
            var sts = opts.ToString();
            if (Mode == 0) return sts.Length;
            var optz = new OptFields();
            optz.MergeFrom(sts);
            Assert.That(opts.Equals(optz), "JSON single values encode-decode");
            return sts.Length;
        }

        //[TestCase(-1)]
        //[TestCase(42)]
        public int SingleValuesNewtonTests(int seed)
        {
            var opts = CreateSingle(seed);
            var sts = JsonConvert.SerializeObject(opts);
            if (Mode == 0) return sts.Length;
            var optz = JsonConvert.DeserializeObject<OptFields>(sts);
            Assert.That(opts.Equals(optz), "JSON.NET single values encode-decode");
            return sts.Length;
        }

        public int SingleValuesDcJsonTests(int seed)
        {
            var opts = CreateSingle(seed);
            var ss = new MemoryStream();
            var js = new DataContractJsonSerializer(typeof(OptFields));
            js.WriteObject(ss, opts);
            if (Mode == 0) return (int)ss.Position;
            ss.Position = 0;
            var optz = js.ReadObject(ss) as OptFields;
            Assert.That(opts.Equals(optz), "DataContract single values encode-decode");
            return (int)ss.Length;
        }

        [TestCase(-1)]
        [TestCase(42)]
        public void RepeatedValuesProtoTestsN(int seed) { RepeatedValuesProtoTests(seed); }
        public int RepeatedValuesProtoTests(int seed)
        {
            var opts = CreateRepeated(seed);
            var bto = opts.ToByteArray();
            if (Mode == 0) return bto.Length;
            var optz = new RepFields();
            optz.MergeFrom(bto);
            Assert.That(opts.Equals(optz), "PB multi values encode-decode");
            return bto.Length;
        }

        [TestCase(125)]
        [TestCase(4200)]
        public void RepeatedValuesJsonTestsN(int seed) { RepeatedValuesJsonTests(seed); }
        public int RepeatedValuesJsonTests(int seed)
        {
            var opts = CreateRepeated(seed);
            var sts = opts.ToString();
            if (Mode == 0) return sts.Length;
            var optz = new RepFields();
            optz.MergeFrom(sts);
            Assert.That(opts.Equals(optz), "JSON multi values encode-decode");
            return sts.Length;
        }

        //[TestCase(125)]
        //[TestCase(4200)]
        public int RepeatedValuesNewtonTests(int seed)
        {
            var opts = CreateRepeated(seed);
            var sts = JsonConvert.SerializeObject(opts);
            if (Mode == 0) return sts.Length;
            var optz = JsonConvert.DeserializeObject<RepFields>(sts);
            Assert.That(opts.Equals(optz), "JSON.NET multi values encode-decode");
            return sts.Length;
        }

        public int RepeatedValuesDcJsonTests(int seed)
        {
            var opts = CreateRepeated(seed);
            var ss = new MemoryStream();
            var js = new DataContractJsonSerializer(typeof(RepFields));
            js.WriteObject(ss, opts);
            if (Mode == 0) return (int)ss.Position;
            ss.Position = 0;
            var optz = js.ReadObject(ss) as RepFields;
            Assert.That(opts.Equals(optz), "Json-Datacontract multi values encode-decode");
            return (int)ss.Length;
        }

        [TestCase(12)]
        [TestCase(1200)]
        public void AggregatedMessageTests(int seed)
        {
            // test aggregate message producing relatively large output.
            var msg = new Noah { Vals = CreateSingle(7), Reps = CreateRepeated(22) };
            for (var i = 0; i < seed; i++)
                msg.AddLots(CreateSingle(i));
            msg.Tags = "abc-def-gfh-ijk-lmn-opq";
            var btn = msg.ToByteArray();
            var chk = new Noah();
            chk.MergeFrom(btn);
            Assert.That(msg.Equals(chk), "PB large message encode-decode");
            chk.Clear();
            var sts = msg.ToString();
            chk.MergeFrom(sts);
            Assert.That(msg.Equals(chk), "JSON large message encode-decode");
        }

        public static void GoogleCsharpComparison(int iMultics = 100)
        {
            // Google Protocol Buffers port to C# shows ~77mb/s write and 40mb/s read speed on this sample file on i5(2500).
            // tests shows negligible difference between managed/unmanaged versions with our PB codecs.
            byte[] odata = System.IO.File.ReadAllBytes("abook.data"), adata = null;
            var abook = new tutorial.AddressBook();
            abook.MergeFrom(odata);
            Message rec = new tutorial.AddressBook();
            adata = abook.ToByteArray();
            rec.MergeFrom(adata);
            var clock = new Stopwatch();
            // test our write speed.
            clock.Restart();
            for (var i = 0; i < 1000 * iMultics; i++)
            {
                adata = rec.ToByteArray(adata, 0, adata.Length);
                //var uw = new PBuStreamWriter( ubt, 0, adata.Length );
                //uw.Write( rec );
            }
            clock.Stop();
            Console.WriteLine("-- pb.write: elapsed time = {0} ms, speed = {1} mb/s", clock.ElapsedMilliseconds, Math.Round((double)(adata.Length * iMultics) / (clock.ElapsedMilliseconds + 1), 2));
            Assert.That(Pbs.EqualBytes(odata, adata), "PB: write check fail");
            // test our read speed.
            clock.Restart();
            for (var i = 0; i < 1000 * iMultics; i++)
            {
                //var pbr = new PBuStreamReader( ubt, 0, adata.Length );
                rec = new tutorial.AddressBook();
                rec.MergeFrom(odata);
                //pbr.Read( rec, adata.Length );
            }
            clock.Stop();
            Assert.That(rec.Equals(abook), "PB: read check fail");
            Console.WriteLine("-- pb.read: elapsed time = {0} ms, speed = {1} mb/s", clock.ElapsedMilliseconds, Math.Round((double)(adata.Length * iMultics) / (clock.ElapsedMilliseconds + 1), 2));
        }

    }
}
