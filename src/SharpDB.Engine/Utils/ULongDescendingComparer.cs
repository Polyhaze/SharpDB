using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SharpDB.Engine.Utils
{
	public class ULongDescendingComparer : IComparer<ulong>
	{
		public int Compare(ulong x, ulong y)
		{
			if (x > y)
			{
				return -1;
			}
			else if (y > x)
			{
				return 1;
			}
			else
			{
				return 0;
			}
		}
	}
}
