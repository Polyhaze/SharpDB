using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using SomDB.Engine;

namespace ConsoleApplication1
{
	class Program
	{
		static void Main(string[] args)		
		{
			Stopwatch stopwatch = Stopwatch.StartNew();

			DB db = new DB("perf.dbfile");
			db.Start();

			stopwatch.Stop();

			Console.WriteLine("Loading db took {0}ms", stopwatch.ElapsedMilliseconds);

			stopwatch.Restart();
			
			db.BackupAsync("backup.dbfile").Wait();

			stopwatch.Stop();

			Console.WriteLine("Backup db took {0}ms", stopwatch.ElapsedMilliseconds);

			stopwatch.Restart();

			DB backup = new DB("backup.dbfile");
			backup.Start();

			stopwatch.Stop();

			Console.WriteLine("Loading backup db took {0}ms", stopwatch.ElapsedMilliseconds);

			byte[] blob = backup.Read("1");

			Console.WriteLine(blob[0]);

			Console.ReadKey();
		}
	}
}
