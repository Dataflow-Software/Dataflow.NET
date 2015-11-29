using System;
using System.ServiceProcess;

namespace Cached.Net
{
    static class ServiceMain
    {
        static void Main(string[] args)
        {
            // running in console mode support.
            if (args.Length > 0 && args[0].StartsWith("-c"))
            {
                var host = new ServiceHost();
                try
                {
                    host.Start(true, args);
                    Console.WriteLine("\n-- cached.net - memcached service implementation for .NET");
                    Console.WriteLine("-- press <enter> to stop service ...\n");
                    Console.ReadLine();
                }
                finally
                {
                    host.Stop();
                    Environment.Exit(0);
                }
            }
            // .NET service initialization part.
            var ServicesToRun = new ServiceBase[] 
            { 
                new CachedService() 
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
