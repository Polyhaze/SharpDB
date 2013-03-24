using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SomDB.Engine.Backup;
using SomDB.Engine.Domain;
using SomDB.Engine.IO;

namespace SomDB.Engine
{
	public class DB
	{
		private DocumentStore m_documentStore;

		private DatabaseFileWriter m_databaseFileWriter;
		private DatabaseFileReader m_databaseFileReader;

		private Cache m_cache;

		private int m_currentTransactionId = 1;

		Dictionary<int, Transaction> m_pendingTransaction = new Dictionary<int, Transaction>();
		
		public DB(string fileName)
		{
			FileName = fileName;

		}

		public ulong DBTimeStamp { get; private set; }

		public string FileName { get; private set; }

		public void Start()
		{
			m_databaseFileWriter = new DatabaseFileWriter(FileName);
			m_databaseFileReader = new DatabaseFileReader(FileName);

			m_cache = new Cache(FileName);

			ulong timestamp;

			m_documentStore = new DocumentStore(m_databaseFileReader.GetDocuments(out timestamp));

			DBTimeStamp = timestamp;
		}

		public void Store(string documentId, byte[] blob)
		{
			Document document = m_documentStore.GetDocumentForUpdate(documentId, -1);

			DBTimeStamp++;

			m_databaseFileWriter.BeginTimestamp(DBTimeStamp, 1);

			long documentLocation = m_databaseFileWriter.WriteDocument(documentId, blob);
			m_cache.Set(documentLocation, blob);

			m_databaseFileWriter.Flush();

			if (document != null)
			{
				document.Update(DBTimeStamp, documentLocation, blob.Length, true);
			}
			else
			{
				m_documentStore.AddNewDocument(documentId, DBTimeStamp, documentLocation, blob.Length);
			}
		}

		public byte[] Read(string key)
		{
			Document document = m_documentStore.GetDocument(key);

			if (document != null)
			{
				return ReadInternal(document.CurrentRevision.BlobFileLocation, document.CurrentRevision.BlobSize);
			}
			else
			{
				return null;
			}
		}

		private byte[] ReadInternal(long fileLocation, int size)
		{
			byte[] blob = m_cache.Get(fileLocation);

			if (blob == null)
			{
				blob = m_databaseFileReader.ReadDocument(fileLocation, size);
				m_cache.Set(fileLocation, blob);
			}

			return blob;
		}

		public int StartTransaction()
		{
			Transaction transaction = new Transaction(m_currentTransactionId, DBTimeStamp);
			m_currentTransactionId++;

			m_pendingTransaction.Add(transaction.TransactionId, transaction);

			return transaction.TransactionId;
		}

		public void TransactionUpdate(int transactionId, string documentId, byte[] blob)
		{
			Transaction transaction;

			if (!m_pendingTransaction.TryGetValue(transactionId, out transaction))
			{
				throw new TransactionNotExistException();
			}

			// mark the document is updated by transaction
			m_documentStore.GetDocumentForUpdate(documentId, transactionId);

			transaction.AddUpdate(documentId, blob);
		}

		public byte[] TransactionRead(int transactionId, string documentId)
		{
			Transaction transaction;

			if (!m_pendingTransaction.TryGetValue(transactionId, out transaction))
			{
				throw new TransactionNotExistException();
			}

			Document document = m_documentStore.GetDocument(documentId);
			if (document == null)
			{
				return null;
			}

			// if updated by current transaction we just return the current blob
			if (document.TransactionId == transactionId)
			{
				return transaction.GetBlob(documentId);
			}

			// we need to find the current revision
			DocumentRevision revision = document.GetDocumentRevisionByTimestamp(transaction.DBTimestamp);

			// if there is no revision the object is not exist for this timestamp
			if (revision == null)
			{
				return null;
			}

			return ReadInternal(revision.BlobFileLocation, revision.BlobSize);
		}

		public void RollbackTransaction(int transactionId)
		{
			Transaction transaction;

			if (!m_pendingTransaction.TryGetValue(transactionId, out transaction))
			{
				throw new TransactionNotExistException();
			}

			foreach (string documentId in transaction.GetDocumentIds())
			{
				Document document = m_documentStore.GetDocument(documentId);
				document.RollbackTransaction();
			}

			m_pendingTransaction.Remove(transactionId);
		}

		public void CommitTransaction(int transactionId)
		{
			Transaction transaction;

			if (!m_pendingTransaction.TryGetValue(transactionId, out transaction))
			{
				throw new TransactionNotExistException();
			}

			if (transaction.Count > 0)
			{
				DBTimeStamp++;

				m_databaseFileWriter.BeginTimestamp(DBTimeStamp, transaction.Count);

				foreach (string documentId in transaction.GetDocumentIds())
				{
					byte[] blob = transaction.GetBlob(documentId);

					long documentLocation = m_databaseFileWriter.WriteDocument(documentId, blob);
					m_cache.Set(documentLocation, blob);

					Document document = m_documentStore.GetDocument(documentId);

					if (document != null)
					{
						document.CommitTransaction(DBTimeStamp, documentLocation, blob.Length);
					}
					else
					{
						m_documentStore.AddNewDocument(documentId, DBTimeStamp, documentLocation, blob.Length);
					}
				}

				m_databaseFileWriter.Flush();
			}

			m_pendingTransaction.Remove(transactionId);
		}

		public void Cleanup()
		{
			ulong minTimestamp = DBTimeStamp;

			if (m_pendingTransaction.Any())
			{
				minTimestamp = m_pendingTransaction.Min(t => t.Value.DBTimestamp);
			}
				
			m_documentStore.Cleanup(minTimestamp);
		}

		public Task BackupAsync(string destinationFilename)
		{
			FullBackup backupProcess = new FullBackup(m_documentStore.GetAllDocumentsLatestRevision(), FileName, destinationFilename);

			return Task.Factory.StartNew(backupProcess.Start);
		}
			
		public void Stop()
		{
			m_databaseFileWriter.Dispose();
			m_databaseFileWriter = null;

			m_databaseFileReader.Dispose();
			m_databaseFileReader = null;

			m_documentStore = null;

			m_pendingTransaction.Clear();
			m_pendingTransaction = null;

			m_currentTransactionId = 0;
			DBTimeStamp = 0;
		}

	}
}
