using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SomDB.Engine;

namespace SomDB.Server
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

			DB db = new DB(databaseFile);
			db.Start();

			Network.Server server = new Network.Server(NetMQ.NetMQContext.Create(), db, port);

			server.Start();
		}
	}
}
