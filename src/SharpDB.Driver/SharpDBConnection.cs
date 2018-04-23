using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Common.Logging;
using NetMQ;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using SharpDB.Shared;

namespace SharpDB.Driver
{
    public class SharpDBConnection : IDisposable
    {
        private bool m_isDisposed;
        private SharpDBClient m_client;
        internal ILog Log { get; private set; }

        private IList<SharpDBTransaction> m_transactions = new List<SharpDBTransaction>();

        internal SharpDBConnection(SharpDBClient client, NetMQSocket socket, ISerializer serializer)
        {
            Log = LogManager.GetLogger(this.GetType());
            m_client = client;
            Socket = socket;
            Serializer = serializer;
            m_isDisposed = false;
        }

        internal NetMQSocket Socket { get; private set; }
        internal ISerializer Serializer { get; private set; }

        public void Dispose()
        {
            lock (this)
            {
                if (!m_isDisposed)
                {
                    foreach (SharpDBTransaction transaction in m_transactions)
                    {
                        transaction.Rollback();
                    }

                    m_transactions.Clear();

                    m_client.ReleaseConnection(this);

                    Socket = null;
                    Serializer = null;
                    m_client = null;

                    m_isDisposed = true;
                }
            }
        }

        private object GetDocumentId<T>(T document)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(object), "documentId");

            MethodInfo methodInfo = typeof(T).GetProperty("Id").GetGetMethod(); ;

            Expression<Func<object, object>> getIdExpression =
                Expression.Lambda<Func<object, object>>(
                Expression.Convert(Expression.Call(Expression.Convert(parameter, methodInfo.DeclaringType), methodInfo), typeof(object)), new[] { parameter });

            return getIdExpression.Compile()(document);
        }

        #region Internal binary methods

        internal byte[] GetInternal(byte[] documentIdBytes, SharpDBTransaction transaction = null)
        {
            if (transaction != null)
            {
                Socket.SendMoreFrame(Protocol.TransactionGet).SendMoreFrame(transaction.TransactionIdBytes);
            }
            else
            {
                Socket.SendMoreFrame(Protocol.Get);
            }

            Socket.SendFrame(documentIdBytes);

            bool result = BitConverter.ToBoolean(Socket.ReceiveFrameBytes(), 0);

            if (result)
            {
                return Socket.ReceiveFrameBytes();
            }
            else
            {
                string error = Socket.ReceiveFrameString();

                throw new SharpDBException(error);
            }
        }

        internal void UpdateInternal(byte[] documentIdBytes, byte[] blob, SharpDBTransaction transaction = null)
        {
            if (transaction != null)
            {
                Socket.SendMoreFrame(Protocol.TransactionUpdate).SendMoreFrame(transaction.TransactionIdBytes);
            }
            else
            {
                Socket.SendMoreFrame(Protocol.Update);
            }

            Socket.SendMoreFrame(documentIdBytes).SendFrame(blob);

            bool result = BitConverter.ToBoolean(Socket.ReceiveFrameBytes(), 0);

            if (!result)
            {
                string error = Socket.ReceiveFrameString();

                throw new SharpDBException(error);
            }
        }

        public void DeleteInternal(byte[] documentIdBytes, SharpDBTransaction transaction = null)
        {
            if (transaction != null)
            {
                Socket.SendMoreFrame(Protocol.TransactionDelete).SendMoreFrame(transaction.TransactionIdBytes);
            }
            else
            {
                Socket.SendMoreFrame(Protocol.Delete);
            }

            Socket.SendFrame(documentIdBytes);

            bool result = BitConverter.ToBoolean(Socket.ReceiveFrameBytes(), 0);

            if (!result)
            {
                string error = Socket.ReceiveFrameString();

                throw new SharpDBException(error);
            }
        }

        #endregion Internal binary methods

        #region Transactionless methods

        public T Get<T>(object documentId)
        {
            byte[] documentIdBytes = Serializer.SerializeDocumentId(documentId);

            byte[] blob = GetInternal(documentIdBytes);

            if (blob.Length > 0)
            {
                return Serializer.DeserializeDocument<T>(blob);
            }
            else
            {
                return default(T);
            }
        }

        public void Update<T>(T document)
        {
            object documentId = GetDocumentId(document);

            byte[] documentIdBytes = Serializer.SerializeDocumentId(documentId);

            byte[] blob = Serializer.SerializeDocument(document);

            UpdateInternal(documentIdBytes, blob);
        }

        public void DeleteDocument<T>(T document)
        {
            DeleteDocumentById(GetDocumentId(document));
        }

        public void DeleteDocumentById(object documentId)
        {
            byte[] documentIdBytes = Serializer.SerializeDocumentId(documentId);

            DeleteInternal(documentIdBytes);
        }

        #endregion Transactionless methods

        #region Transaction methods

        internal T TransactionGet<T>(SharpDBTransaction transaction, object documentId)
        {
            byte[] documentIdBytes = Serializer.SerializeDocumentId(documentId);

            byte[] blob = GetInternal(documentIdBytes, transaction);

            if (blob.Length > 0)
            {
                return Serializer.DeserializeDocument<T>(blob);
            }
            else
            {
                return default(T);
            }
        }

        internal void TransactionUpdate<T>(SharpDBTransaction transaction, T document)
        {
            object documentId = GetDocumentId(document);
            byte[] documentIdBytes = Serializer.SerializeDocumentId(documentId);

            byte[] blob = Serializer.SerializeDocument(document);

            UpdateInternal(documentIdBytes, blob, transaction);
        }

        internal void TransactionDeleteDocument<T>(SharpDBTransaction transaction, T document)
        {
            TransactionDeleteDocumentById(transaction, GetDocumentId(document));
        }

        internal void TransactionDeleteDocumentById(SharpDBTransaction transaction, object documentId)
        {
            byte[] documentIdBytes = Serializer.SerializeDocumentId(documentId);

            DeleteInternal(documentIdBytes, transaction);
        }

        public SharpDBTransaction StartTransaction()
        {
            Socket.SendMoreFrame(Protocol.StartTransaction);

            bool result = BitConverter.ToBoolean(Socket.ReceiveFrameBytes(), 0);

            if (result)
            {
                int transactionId = BitConverter.ToInt32(Socket.ReceiveFrameBytes(), 0);

                Log.DebugFormat("Transaction {0} started", transactionId);

                return new SharpDBTransaction(this, transactionId);
            }
            else
            {
                string error = Socket.ReceiveFrameString();

                Log.ErrorFormat("Failed to start transaction, error: {0}", error);

                throw new SharpDBException(Socket.ReceiveFrameString());
            }
        }

        internal void CommitTransaction(SharpDBTransaction transaction)
        {
            Socket.SendMoreFrame(Protocol.Commit).SendFrame(transaction.TransactionIdBytes);

            bool result = BitConverter.ToBoolean(Socket.ReceiveFrameBytes(), 0);

            m_transactions.Remove(transaction);

            if (!result)
            {
                string error = Socket.ReceiveFrameString();

                Log.ErrorFormat("Failed to commit transaction {0}, error: {1}", transaction.TransactionId, error);

                throw new SharpDBException(error);
            }
            else
            {
                Log.DebugFormat("Transaction {0} committed", transaction.TransactionId);
            }
        }

        internal void RollbackTransaction(SharpDBTransaction transaction)
        {
            Socket.SendMoreFrame(Protocol.Rollback).SendFrame(transaction.TransactionIdBytes);

            bool result = BitConverter.ToBoolean(Socket.ReceiveFrameBytes(), 0);
            m_transactions.Remove(transaction);

            if (!result)
            {
                string error = Socket.ReceiveFrameString();

                Log.ErrorFormat("Failed to rollback transaction {0}, error: {1}", transaction.TransactionId, error);

                throw new SharpDBException(error);
            }
            else
            {
                Log.DebugFormat("Transaction {0} rollbacked", transaction.TransactionId);
            }
        }

        #endregion Transaction methods
    }
}