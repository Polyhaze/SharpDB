using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SomDB.Shared
{
	public static class Protocol
	{
		public static byte[] Success = BitConverter.GetBytes(true);
		public static byte[] Failed = BitConverter.GetBytes(false);
 
	}
}
