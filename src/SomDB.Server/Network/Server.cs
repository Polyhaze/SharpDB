using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetMQ;
using SomDB.Engine;
using SomDB.Shared;

namespace SomDB.Server.Network
{
	public class Server
	{
		private readonly NetMQContext m_context;
		private readonly DB m_db;
		private readonly int m_port;
		private NetMQSocket m_serverSocket;
		private Poller m_poller;

		public Server(NetMQContext context, DB db, int port)
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
				case MessageType.Read:
					Read();
					break;
				case MessageType.Update:
					Update();
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
				case MessageType.TransactionRead:
					TransactionRead();					
					break;
				case MessageType.TransactionUpdate:
					TransactionUpdate();
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		private void TransactionUpdate()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			byte[] documentIdBytes = m_serverSocket.Receive();

			string documentId = Encoding.ASCII.GetString(documentIdBytes);

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

		private void TransactionRead()
		{
			byte[] transactionIdBytes = m_serverSocket.Receive();

			int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

			byte[] documentIdBytes = m_serverSocket.Receive();

			string documentId = Encoding.ASCII.GetString(documentIdBytes);

			try
			{
				byte [] blob = m_db.TransactionRead(transactionId, documentId);

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

			string documentId = Encoding.ASCII.GetString(documentIdBytes);

			byte[] blob = m_serverSocket.Receive();

			try
			{
				m_db.Store(documentId, blob);

				// sending success
				m_serverSocket.Send(Protocol.Success);
			}
			catch (DocumentLockedException)
			{
				m_serverSocket.SendMore(Protocol.Failed).Send("Document locked by another transaction");				
			}			
		}

		private void Read()
		{
			byte[] documentIdBytes = m_serverSocket.Receive();

			string documentId = Encoding.ASCII.GetString(documentIdBytes);

			byte[] blob = m_db.Read(documentId);

			if (blob == null)
			{
				blob = new byte[0];
			}

			m_serverSocket.SendMore(Protocol.Success).Send(blob);
		}
	}
}
