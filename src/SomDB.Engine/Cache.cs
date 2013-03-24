using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Caching;
using System.Text;

namespace SomDB.Engine
{
	public class Cache
	{
		private MemoryCache m_memoryCache;
		private CacheItemPolicy m_policy;

		public Cache(string filename)
		{
			Filename = filename;
			m_memoryCache = new MemoryCache(Filename);			
		
			m_policy = new CacheItemPolicy();
			m_policy.SlidingExpiration = TimeSpan.FromHours(1);
		}

		public string Filename { get; set; }

		public void Set(long fileLocation, byte[] blob)
		{
			string key = ConvertLongToString(fileLocation);
			
			m_memoryCache.Set(key, blob, m_policy);
		}

		private string ConvertLongToString(long fileLocation)
		{
			// converting the long to string
			byte[] bytes = BitConverter.GetBytes(fileLocation);

			return Encoding.ASCII.GetString(bytes);
		}

		public byte[] Get(long fileLocation)
		{
			string key = ConvertLongToString(fileLocation);

			return (byte[])m_memoryCache.Get(key);
		}

		public void Clear()
		{
			m_memoryCache.Dispose();
			m_memoryCache = new MemoryCache(Filename);
		}
	}
}
