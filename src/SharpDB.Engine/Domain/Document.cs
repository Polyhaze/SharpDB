using System.Collections.Generic;
using System.Linq;
using SharpDB.Engine.Utils;

namespace SharpDB.Engine.Domain
{
	public class Document
	{
		private SortedList<ulong, DocumentRevision> m_revisions = 
			new SortedList<ulong, DocumentRevision>(new ULongDescendingComparer());

		public Document(DocumentId documentId, ulong documentTimeStamp, long blobFileLocation, int blobSize)
		{
			DocumentId = documentId;

			CurrentRevision = new DocumentRevision(documentId,documentTimeStamp, blobFileLocation, blobSize);
		}

		public DocumentId DocumentId { get; private set; }

		public DocumentRevision CurrentRevision { get; private set; }
		
		public int TransactionId { get; private set; }
		
		public bool IsLocked {get { return TransactionId != 0; }}

		public void Update(ulong documentTimeStamp, long blobFileLocation, int blobSize, bool saveRevision)
		{
			CurrentRevision.Expire(documentTimeStamp);

			if (saveRevision)
			{				
				m_revisions.Add(CurrentRevision.TimeStamp, CurrentRevision);
			}

			CurrentRevision = new DocumentRevision(DocumentId, documentTimeStamp, blobFileLocation, blobSize);			
		}

		public void TransactionLock(int transactionId)
		{
			TransactionId = transactionId;
		}

		public void RollbackTransaction()
		{
			TransactionId = 0;
		}

		public void CommitTransaction(ulong documentTimeStamp, long blobFileLocation, int blobSize)
		{
			TransactionId = 0;

			Update(documentTimeStamp, blobFileLocation, blobSize, true);
		}

		public DocumentRevision GetDocumentRevisionByTimestamp(ulong timestamp)
		{
			if (timestamp >= CurrentRevision.TimeStamp)
			{
				return CurrentRevision;
			}
			else
			{
				foreach (KeyValuePair<ulong, DocumentRevision> documentRevision in m_revisions)
				{
					if (timestamp >= documentRevision.Value.TimeStamp && timestamp < documentRevision.Value.ExpireTimeStamp)
					{
						return documentRevision.Value;
					}
				}
				
				return null;
			}
		}

		public List<DocumentRevision> GetRevisionsAboveTimestamp(ulong timestamp)
		{
			var list = m_revisions.Where(r => r.Key > timestamp).Select(r=> r.Value).ToList();

			if (CurrentRevision.TimeStamp > timestamp)
			{
				list.Add(CurrentRevision);
			}

			return list;			
		}

		public void Cleanup(ulong timestamp)
		{
			// find all the revisions with timestamp lower than the minimum timestamp
			ulong[] keys = m_revisions.Keys.Where(k => k < timestamp).ToArray();

			foreach (ulong key in keys)
			{
				m_revisions.Remove(key);
			}
		}
	}
}
