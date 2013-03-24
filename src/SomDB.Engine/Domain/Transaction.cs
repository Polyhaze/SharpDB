using System.Collections.Generic;

namespace SomDB.Engine.Domain
{
	public class Transaction
	{
		Dictionary<string, byte[]> m_updates = new Dictionary<string, byte[]>(); 		

		public Transaction(int transactionId, ulong dbTimestamp)
		{
			TransactionId = transactionId;
			DBTimestamp = dbTimestamp;
		}

		public int TransactionId { get; private set; }
		public ulong DBTimestamp { get; private set; }

		public int Count
		{
			get { return m_updates.Count; }
		}

		public void AddUpdate(string documentId, byte[] blob)
		{
			m_updates.Add(documentId, blob);
		}

		public IEnumerable<string> GetDocumentIds()
		{
			return m_updates.Keys;
		}

		public byte[] GetBlob(string documentId)
		{
			return m_updates[documentId];
		}
	}
}
