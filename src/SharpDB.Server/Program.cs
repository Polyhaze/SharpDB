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

        private static void AddArgumentsToPath(string serviceName, string parameters)
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                string registryPath = @"SYSTEM\CurrentControlSet\Services\" + serviceName;
                RegistryKey keyHKLM = Registry.LocalMachine;

                RegistryKey key = keyHKLM.OpenSubKey(registryPath, true);

                string value = key.GetValue("ImagePath").ToString();

                key.SetValue("ImagePath", value + " " + parameters);

                key.Close();
            }
        }
    }
}