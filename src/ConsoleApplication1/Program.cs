using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using NetMQ;
using Newtonsoft.Json.Linq;
using SomDB.Driver;
using SomDB.Driver.Json;

namespace ConsoleApplication1
{
	class Program
	{
		class Account
		{
			public Account(int id, string name)
			{
				Id = id;
				Name = name;
			}

			public int Id { get; set; }

			public string Name { get; set; }
		}


		static void Main(string[] args)
		{
			Thread.Sleep(2000);

			using (NetMQContext context = NetMQContext.Create())
			{
				SomDBClient client = new SomDBClient(context, "tcp://127.0.0.1:5999");

				var connection = client.GetConnection();

				connection.Update(new Account(1, "Doron"));

				var jobject = connection.GetJObject(1);

				JObject obj = new JObject();

				JValue idValue = new JValue(3);
				JValue nameValue = new JValue("Yoni");

				obj.Add("Id", idValue);
				obj.Add("Name", nameValue);

				connection.UpdateJObject(obj);

				var account = connection.Get<Account>(3);

				Console.WriteLine("Id:{0}, Name:{1}", account.Id, account.Name);				
			}
			Console.ReadKey();
		}
	}
}
