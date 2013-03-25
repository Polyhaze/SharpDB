using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SomDB.Shared
{
	public enum MessageType : byte
	{
		Get, Update,Delete, StartTransaction, Commit, Rollback, TransactionGet, TransactionUpdate, TransactionDelete 
	}
}
