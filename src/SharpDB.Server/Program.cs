using System;
using Topshelf;

namespace SharpDB.Server
{
    internal class Program
    {
        private static int Main(string[] args)
        {
            string databaseFile = "default";
            int port = 5999;
            return (int)HostFactory.Run(x =>
                   {
                       x.UseAssemblyInfoForServiceInfo();

                       x.Service(settings => new ServerService(databaseFile, port), s =>
{
    s.BeforeStartingService(_ => Console.WriteLine("BeforeStart"));
    s.BeforeStoppingService(_ => Console.WriteLine("BeforeStop"));
});

                       x.SetStartTimeout(TimeSpan.FromSeconds(10));
                       x.SetStopTimeout(TimeSpan.FromSeconds(10));

                       x.AddCommandLineDefinition("name", f => databaseFile = f);
                       x.AddCommandLineDefinition("port", p => port = Convert.ToInt32(p));

                       x.OnException((exception) =>
               {
                   Console.WriteLine("Exception thrown - " + exception.Message);
               });
                   });
        }
    }
}