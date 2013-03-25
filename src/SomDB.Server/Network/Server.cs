using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetMQ;
using SomDB.Engine;
using SomDB.Engine.Domain;
using SomDB.Shared;

namespace SomDB.Server.Network
{
	public class Server
	{
		private readonly NetMQContext m_context;
		private readonly KetValueDatabase m_db;
		private readonly int m_port;
		private NetMQSocket m_serverSocket;
		private Poller m_poller;

		public Server(NetMQContext context, KetValueDatabase db, int port)
		{
			m_context = context;
			m_db = db;
			m_port = port;
		}

		public void Start()
		{
			using (m_serverSocket = m_context.CreateResponseSocket())
			{
				m_serverSocket.Bind(string.Format("tcp://*:{0}", m_port));
				m_serverSocket.ReceiveReady += OnMessage;

				m_poller = new Poller();
				m_poller.AddSocket(m_serverSocket);

				m_poller.Start();
			}
		}

		private void OnMessage(object sender, NetMQSocketEventArgs e)
		{
			byte[] messageTypeBytes = m_serverSocket.Receive();

			MessageType messageType = (MessageType) messageTypeBytes[0];

			switch (messageType)
			{
				case MessageType.Get:
					Get();
					break;
				case MessageType.Update:
					Update();
					break;
				case MessageType.Delete:
					Delete();
					break;					
				case MessageType.StartTransaction:
					StartTransaction();
					break;
				case MessageType.Commit:
					Commit();
					break;
				case MessageType.Rollback:
					Rollback();
					break;
				case MessageType.TransactionGet:
					TransactionGet();					
					break;
				case MessageType.TransactionUpdate:
					TransactionUpdate();
					break;
				case MessageType.TransactionDelete:
					TransactionDelete();
					break;					
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		
		private void TransactionDelete()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			byte[] documentIdBytes = m_serverSocket.Receive();

			DocumentId documentId = new DocumentId(documentIdBytes);
			
			try
			{
				m_db.TransactionDelete(transactionId, documentId);

				// sending success
				m_serverSocket.Send(Protocol.Success);
			}
			catch (TransactionNotExistException ex)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Transaction doesn't exist");
			}
			catch (DocumentLockedException)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Document locked by another transaction");
			}
		}


		private void TransactionUpdate()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			byte[] documentIdBytes = m_serverSocket.Receive();

			DocumentId documentId = new DocumentId(documentIdBytes);

			byte[] blob = m_serverSocket.Receive();

			try
			{
				m_db.TransactionUpdate(transactionId, documentId, blob);

				// sending success
				m_serverSocket.Send(Protocol.Success);			
			}
			catch (TransactionNotExistException ex)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Transaction doesn't exist");
			}
			catch (DocumentLockedException)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Document locked by another transaction");
			}					
		}

		private void TransactionGet()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			byte[] documentIdBytes = m_serverSocket.Receive();

			DocumentId documentId = new DocumentId(documentIdBytes);

			try
			{
				byte [] blob = m_db.TransactionGet(transactionId, documentId);

				if (blob == null)
				{
					blob = new byte[0];
				}

				m_serverSocket.SendMore(Protocol.Success).Send(blob);
			}
			catch (TransactionNotExistException ex)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Transaction doesn't exist");
			}
		}

		private void Rollback()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			try
			{
				m_db.RollbackTransaction(transactionId);
				m_serverSocket.Send(Protocol.Success);
			}
			catch (TransactionNotExistException ex)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Transaction doesn't exist");
			}
		}

		private void Commit()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			try
			{
				m_db.CommitTransaction(transactionId);
				m_serverSocket.Send(Protocol.Success);
			}
			catch (TransactionNotExistException ex)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Transaction doesn't exist");				
			}			
		}

		private void StartTransaction()
		{
			int transactionId = m_db.StartTransaction();

			m_serverSocket.SendMore(Protocol.Success).Send(BitConverter.GetBytes(transactionId));
		}

		private void Update()
		{
			byte[] documentIdBytes = m_serverSocket.Receive();

			DocumentId documentId = new DocumentId(documentIdBytes);

			byte[] blob = m_serverSocket.Receive();

			try
			{
				m_db.Update(documentId, blob);

				// sending success
				m_serverSocket.Send(Protocol.Success);
			}
			catch (DocumentLockedException)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Document locked by another transaction");				
			}			
		}


		private void Delete()
		{
			byte[] documentIdBytes = m_serverSocket.Receive();

			DocumentId documentId = new DocumentId(documentIdBytes);
			
			try
			{
				m_db.Delete(documentId);

				// sending success
				m_serverSocket.Send(Protocol.Success);
			}
			catch (DocumentLockedException)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Document locked by another transaction");
			}
		}


		private void Get()
		{
			byte[] documentIdBytes = m_serverSocket.Receive();

			DocumentId documentId = new DocumentId(documentIdBytes);

			byte[] blob = m_db.Get(documentId);

			if (blob == null)
			{
				blob = new byte[0];
			}

			m_serverSocket.SendMore(Protocol.Success).Send(blob);
		}
	}
}
