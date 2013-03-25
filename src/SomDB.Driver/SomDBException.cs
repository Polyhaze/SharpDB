using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SomDB.Driver
{
public class SomDBException : Exception
	{
		public SomDBException(string message) : base(message)
		{
		}
	}
}
