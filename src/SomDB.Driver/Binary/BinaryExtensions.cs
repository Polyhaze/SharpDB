using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;

namespace SomDB.Driver.Binary
{
	public static class BinaryExtensions
	{
		public static byte[] GetBinary(this SomDBConnection connection, byte[] documentIdBytes)
		{			
			return connection.GetInternal(documentIdBytes);
		}

		public static void UpdateBinary(this SomDBConnection connection, byte[] documentIdBytes, byte[] blob)
		{
			connection.UpdateInternal(documentIdBytes, blob);
		}

		public static void DeleteBinary(this SomDBConnection connection, byte[] documentIdBytes)
		{
			connection.DeleteInternal(documentIdBytes);
		}

		public static byte[] GetBinary(this SomDBTransaction transaction, byte[] documentIdBytes)
		{
			return transaction.Connection.GetInternal(documentIdBytes, transaction);
		}

		public static void UpdateBinary(this SomDBTransaction transaction, byte[] documentIdBytes, byte[] blob)
		{
			transaction.Connection.UpdateInternal(documentIdBytes, blob, transaction);
		}

		public static void DeleteBinary(this SomDBTransaction transaction, byte[] documentIdBytes)
		{
			transaction.Connection.DeleteInternal(documentIdBytes, transaction);
		}

	}
}
