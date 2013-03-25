using System.Collections.Generic;

namespace SomDB.Engine.Domain
{
	public class Transaction
	{
		Dictionary<DocumentId, byte[]> m_updates = new Dictionary<DocumentId, byte[]>(); 		

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

		public void AddUpdate(DocumentId documentId, byte[] blob)
		{
			m_updates.Add(documentId, blob);
		}

		public IEnumerable<DocumentId> GetDocumentIds()
		{
			return m_updates.Keys;
		}

		public byte[] GetBlob(DocumentId documentId)
		{
			return m_updates[documentId];
		}
	}
}
