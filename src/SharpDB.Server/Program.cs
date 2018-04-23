using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using Microsoft.Win32;
using SharpDB.Engine;
using SharpDB.Engine.IO;
using SharpDB.Engine.Cache;
using Topshelf;
using NetMQ;
using Topshelf.Hosts;
using Common.Logging;

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