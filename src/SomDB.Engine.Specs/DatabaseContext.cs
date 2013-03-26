using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using SomDB.Engine.Cache;
using SomDB.Engine.IO;

namespace SomDB.Engine.Specs
{
	public class DatabaseContext : IDisposable
	{
		public const string SpecsDbFile = "specs.dbfile";

		public DatabaseContext()
		{
			File.Delete(SpecsDbFile);

			Database = new KeyValueDatabase(filename => new DatabaseFileReader(filename), filename => new DatabaseFileWriter(filename),
			                                  filename => new MemoryCacheProvider(filename));
			Database.FileName = SpecsDbFile;
			Database.Start();
		}
		
		public KeyValueDatabase Database { get;  private set; }

		public void Restart()
		{
			Database.Stop();
			Database = new KeyValueDatabase(filename => new DatabaseFileReader(filename), filename => new DatabaseFileWriter(filename),
																				filename => new MemoryCacheProvider(filename));
			Database.FileName = SpecsDbFile;
			Database.Start();
		}

		public void Dispose()
		{
			Database.Stop();			
		}
	}
}
