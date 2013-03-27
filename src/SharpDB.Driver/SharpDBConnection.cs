using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
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
		
		private IList<SharpDBTransaction> m_transactions = new List<SharpDBTransaction>();		

		internal SharpDBConnection(SharpDBClient client, NetMQSocket socket, ISerializer serializer)
		{
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
				Socket.SendMore(Protocol.TransactionGet).SendMore(transaction.TransactionIdBytes);
			}
			else
			{
				Socket.SendMore(Protocol.Get);	
			}			

			Socket.Send(documentIdBytes);

			bool result = BitConverter.ToBoolean(Socket.Receive(), 0);

			if (result)
			{
				return Socket.Receive();				
			}
			else
			{
				string error = Socket.ReceiveString();

				throw new SharpDBException(error);
			}
		}

		internal void UpdateInternal(byte[] documentIdBytes, byte[] blob, SharpDBTransaction transaction = null)
		{
			if (transaction != null)
			{
				Socket.SendMore(Protocol.TransactionUpdate).SendMore(transaction.TransactionIdBytes);
			}
			else
			{
				Socket.SendMore(Protocol.Update);
			}

			Socket.SendMore(documentIdBytes).Send(blob);

			bool result = BitConverter.ToBoolean(Socket.Receive(), 0);

			if (!result)
			{
				string error = Socket.ReceiveString();

				throw new SharpDBException(error);
			}
		}

		public void DeleteInternal(byte[] documentIdBytes, SharpDBTransaction transaction = null)
		{
			if (transaction != null)
			{
				Socket.SendMore(Protocol.TransactionDelete).SendMore(transaction.TransactionIdBytes);
			}
			else
			{
				Socket.SendMore(Protocol.Delete);
			}

			Socket.Send(documentIdBytes);

			bool result = BitConverter.ToBoolean(Socket.Receive(), 0);

			if (!result)
			{
				string error = Socket.ReceiveString();

				throw new SharpDBException(error);
			}
		}

		#endregion

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

		#endregion

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
			Socket.Send(Protocol.StartTransaction);

			bool result = BitConverter.ToBoolean(Socket.Receive(), 0);

			if (result)
			{
				int transactionId = BitConverter.ToInt32(Socket.Receive(), 0);
				return new SharpDBTransaction(this, transactionId);
			}
			else
			{
				throw new SharpDBException(Socket.ReceiveString());
			}
		}

		internal void CommitTransaction(SharpDBTransaction transaction)
		{
			Socket.SendMore(Protocol.Commit).Send(transaction.TransactionIdBytes);

			bool result = BitConverter.ToBoolean(Socket.Receive(), 0);

			m_transactions.Remove(transaction);

			if (!result)
			{
				string error = Socket.ReceiveString();

				throw new SharpDBException(error);
			}
		}

		internal void RollbackTransaction(SharpDBTransaction transaction)
		{
			Socket.SendMore(Protocol.Rollback).Send(transaction.TransactionIdBytes);

			bool result = BitConverter.ToBoolean(Socket.Receive(), 0);
			m_transactions.Remove(transaction);

			if (!result)
			{
				string error = Socket.ReceiveString();

				throw new SharpDBException(error);
			}
		}

		#endregion
	}
}
