using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SharpDB.Engine;
using SharpDB.Engine.IO;
using SharpDB.Engine.Cache;

namespace SharpDB.Server
{
	class Program
	{
		static void Main(string[] args)
		{
			string databaseFile = args[0];

			int port = 5999;

			if (args.Length > 1)
			{
				port = Convert.ToInt32(args[1]);
			}

			KeyValueDatabase db = new KeyValueDatabase(filename => new DatabaseFileReader(filename), filename=> new DatabaseFileWriter(filename),
				filename => new MemoryCacheProvider(filename));
			db.FileName = databaseFile;
			db.Start();

			Network.Server server = new Network.Server(NetMQ.NetMQContext.Create(), db, port);

			server.Start();
		}
	}
}
