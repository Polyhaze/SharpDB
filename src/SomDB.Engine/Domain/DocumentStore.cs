using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SomDB.Engine.Domain
{
	public class DocumentStore
	{
		Dictionary<DocumentId, Document> m_documents;

		public DocumentStore(Dictionary<DocumentId, Document> documents)
		{
			m_documents = documents;
		}

		public Document GetDocumentForUpdate(DocumentId documentId, int transactionId)
		{
			Document document;

			if (m_documents.TryGetValue(documentId, out document) && document.IsLocked && transactionId != document.TransactionId)
			{
				throw new DocumentLockedException();
			}
			else if (document != null && document.TransactionId == 0 && transactionId != -1)
			{
				document.TransactionLock(transactionId);
			}

			return document;
		}

		public Document GetDocument(DocumentId documentId)
		{
			Document document;

			m_documents.TryGetValue(documentId, out document);

			return document;
		}

		public void AddNewDocument(DocumentId documentId, ulong documentTimeStamp, long blobFileLocation, int blobSize)
		{
			m_documents.Add(documentId, new Document(documentId, documentTimeStamp, blobFileLocation, blobSize));
		}

		public void Cleanup(ulong timestamp)
		{
			foreach (KeyValuePair<DocumentId, Document> keyValuePair in m_documents)
			{
				keyValuePair.Value.Cleanup(timestamp);
			}
		}

		public IList<DocumentRevision> GetAllDocumentsLatestRevision()
		{
			return m_documents.Values.Select(d => d.CurrentRevision).ToList();
		}

		public IList<DocumentRevision> GetAllRevisionAboveTimestamp(ulong timestamp)
		{
			return m_documents.Values.SelectMany(d => d.GetRevisionsAboveTimestamp(timestamp)).ToList();
		}
	}
}
