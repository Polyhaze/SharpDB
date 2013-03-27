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
	class Program
	{
		static void Main(string[] args)
		{
			HostFactory.Run(c =>
				{
					string databaseFile = "default";
					int port = 5999;

					c.AddCommandLineDefinition("name", f => databaseFile = f);
					c.AddCommandLineDefinition("port", p => port = Convert.ToInt32(p));

					c.Service<ServerService>(x =>
						{
							x.ConstructUsing(name => new ServerService(databaseFile, port));
							x.WhenStarted(dbService => dbService.Start());
							x.WhenStopped(dbService => dbService.Stop());
						}).RunAsLocalService();

					c.AfterInstall(h =>
						{
							string arguments = string.Format("-name:{0} -port:{1}", databaseFile, port);
							AddArgumentsToPath(h.ServiceName, arguments);
						});
				});			
		}

		private static void AddArgumentsToPath(string serviceName, string parameters)
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
