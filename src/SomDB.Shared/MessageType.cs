using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SomDB.Shared
{
	public enum MessageType : byte
	{
		Read, Update, StartTransaction, Commit, Rollback, TransactionRead, TransactionUpdate 
	}
}
