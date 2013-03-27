using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SharpDB.Shared
{
	public enum MessageType : byte
	{
		Get, Update,Delete, StartTransaction, Commit, Rollback, TransactionGet, TransactionUpdate, TransactionDelete 
	}
}
