using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SomDB.Shared
{
	public static class Protocol
	{
		public static readonly byte[] Success = BitConverter.GetBytes(true);
		public static readonly byte[] Failed = BitConverter.GetBytes(false);


		public static readonly byte[] Get = new byte[] { (byte)MessageType.Get};
		public static readonly byte[] Update = new byte[] { (byte)MessageType.Update };
		public static readonly byte[] Delete = new byte[] { (byte)MessageType.Delete };
		public static readonly byte[] StartTransaction = new byte[] { (byte)MessageType.StartTransaction };
		public static readonly byte[] Commit = new byte[] { (byte)MessageType.Commit };
		public static readonly byte[] Rollback = new byte[] { (byte)MessageType.Rollback };
		public static readonly byte[] TransactionGet = new byte[] { (byte)MessageType.TransactionGet };
		public static readonly byte[] TransactionUpdate = new byte[] { (byte)MessageType.TransactionUpdate };
		public static readonly byte[] TransactionDelete = new byte[] { (byte)MessageType.TransactionDelete };

	}
}
