using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Logging;
using SharpDB.Engine.Domain;
using SharpDB.Engine.IO;
using SharpDB.Engine.Cache;

namespace SharpDB.Engine
{
    public class KeyValueDatabase
    {
        private readonly byte[] ZeroBlob = new byte[0];

        private readonly ILog m_log;
        private readonly Func<string, IDatabaseReader> m_readerFactory;
        private readonly Func<string, IDatabaseWriter> m_writerFactory;
        private readonly Func<string, ICacheProvider> m_cacheProviderFactory;

        private DocumentStore m_documentStore;

        private IDatabaseWriter m_databaseFileWriter;
        private IDatabaseReader m_databaseFileReader;
        private ICacheProvider m_cacheProvider;

        private int m_currentTransactionId = 1;

        private Dictionary<int, Transaction> m_pendingTransaction = new Dictionary<int, Transaction>();

        public KeyValueDatabase(Func<string, IDatabaseReader> readerFactory,
            Func<string, IDatabaseWriter> writerFactory, Func<string, ICacheProvider> cacheProviderFactory)
        {
            m_log = LogManager.GetLogger(this.GetType());

            FileName = "default.dbfile";
            m_readerFactory = readerFactory;
            m_writerFactory = writerFactory;
            m_cacheProviderFactory = cacheProviderFactory;
        }

        public ulong DBTimeStamp { get; private set; }

        public string FileName { get; set; }

        public void Start()
        {
            m_databaseFileWriter = m_writerFactory(FileName);
            m_databaseFileReader = m_readerFactory(FileName);

            m_cacheProvider = m_cacheProviderFactory(FileName);

            ulong timestamp;

            m_documentStore = new DocumentStore(m_databaseFileReader.GetDocuments(out timestamp));

            DBTimeStamp = timestamp;
        }

        public void Update(DocumentId documentId, byte[] blob)
        {
            Document document;
            try
            {
                document = m_documentStore.GetDocumentForUpdate(documentId, -1);
            }
            catch (DocumentLockedException)
            {
                m_log.InfoFormat("Update failed because document is locked by another transaction, documentId(bytes):{0}",
                    documentId.GetBytesReprestnation());

                throw;
            }

            DBTimeStamp++;

            m_databaseFileWriter.BeginTimestamp(DBTimeStamp, 1);

            long documentLocation = m_databaseFileWriter.WriteDocument(documentId, blob);

            if (blob.Length > 0)
            {
                m_cacheProvider.Set(documentLocation, blob);
            }

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

        public byte[] Get(DocumentId key)
        {
            Document document = m_documentStore.GetDocument(key);

            if (document != null)
            {
                return ReadInternal(document.CurrentRevision.BlobFileLocation, document.CurrentRevision.BlobSize);
            }
            else
            {
                return ZeroBlob;
            }
        }

        public void Delete(DocumentId documentId)
        {
            Update(documentId, ZeroBlob);
        }

        private byte[] ReadInternal(long fileLocation, int size)
        {
            if (size > 0)
            {
                byte[] blob = m_cacheProvider.Get(fileLocation);

                if (blob == null)
                {
                    blob = m_databaseFileReader.ReadDocument(fileLocation, size);
                    m_cacheProvider.Set(fileLocation, blob);
                }

                return blob;
            }
            else
            {
                return ZeroBlob;
            }
        }

        public int StartTransaction()
        {
            Transaction transaction = new Transaction(m_currentTransactionId, DBTimeStamp);
            m_currentTransactionId++;

            m_pendingTransaction.Add(transaction.TransactionId, transaction);

            m_log.DebugFormat("Start transaction {0}", transaction.TransactionId);

            return transaction.TransactionId;
        }

        public void TransactionUpdate(int transactionId, DocumentId documentId, byte[] blob)
        {
            Transaction transaction;

            if (!m_pendingTransaction.TryGetValue(transactionId, out transaction))
            {
                throw new TransactionNotExistException();
            }

            try
            {
                // mark the document is updated by transaction
                m_documentStore.GetDocumentForUpdate(documentId, transactionId);
            }
            catch (DocumentLockedException)
            {
                m_log.InfoFormat("Tranasction {1} update failed because document is locked by another transaction, documentId(bytes):{0}",
                    documentId.GetBytesReprestnation(), transactionId);

                throw;
            }

            transaction.AddUpdate(documentId, blob);
        }

        public void TransactionDelete(int transactionId, DocumentId documentId)
        {
            TransactionUpdate(transactionId, documentId, ZeroBlob);
        }

        public byte[] TransactionGet(int transactionId, DocumentId documentId)
        {
            Transaction transaction;

            if (!m_pendingTransaction.TryGetValue(transactionId, out transaction))
            {
                throw new TransactionNotExistException();
            }

            Document document = m_documentStore.GetDocument(documentId);
            if (document == null)
            {
                return ZeroBlob;
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
                return ZeroBlob;
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

            foreach (DocumentId documentId in transaction.GetDocumentIds())
            {
                Document document = m_documentStore.GetDocument(documentId);
                document.RollbackTransaction();
            }

            m_pendingTransaction.Remove(transactionId);

            m_log.DebugFormat("Transaction {0} rollbacked", transaction.TransactionId);
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

                foreach (DocumentId documentId in transaction.GetDocumentIds())
                {
                    byte[] blob = transaction.GetBlob(documentId);

                    long documentLocation = m_databaseFileWriter.WriteDocument(documentId, blob);

                    // we don't store deleted objects in the cache
                    if (blob.Length > 0)
                    {
                        m_cacheProvider.Set(documentLocation, blob);
                    }

                    Document document = m_documentStore.GetDocument(documentId);

                    if (document != null)
                    {
                        document.CommitTransaction(DBTimeStamp, documentLocation, blob.Length);
                    }
                    else
                    {
                        // only add the new document if the document is not deleted
                        if (blob.Length > 0)
                        {
                            m_documentStore.AddNewDocument(documentId, DBTimeStamp, documentLocation, blob.Length);
                        }
                    }
                }

                m_databaseFileWriter.Flush();
            }

            m_pendingTransaction.Remove(transactionId);

            m_log.DebugFormat("Transaction {0} committed", transaction.TransactionId);
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