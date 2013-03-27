using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SharpDB.Driver
{
public class SharpDBException : Exception
	{
		public SharpDBException(string message) : base(message)
		{
		}
	}
}
