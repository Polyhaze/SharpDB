using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Common.Logging;
using NetMQ;
using SharpDB.Engine;
using SharpDB.Engine.Domain;
using SharpDB.Shared;

namespace SharpDB.Server.Network
{
    public class Server
    {
        private readonly KeyValueDatabase m_db;
        private readonly string[] m_addresses;
        private NetMQSocket m_serverSocket;
        private NetMQPoller m_poller;
        private ILog m_log;

        public Server(KeyValueDatabase db, params string[] addresses)
        {
            m_db = db;
            m_addresses = addresses;
            m_log = LogManager.GetLogger(this.GetType());

            if (!m_addresses.Any())
            {
                throw new ArgumentException("You must provide at least one address to listen too");
            }
        }

        public void Start()
        {
            using (m_serverSocket = new NetMQ.Sockets.ResponseSocket())
            {
                foreach (var address in m_addresses)
                {
                    m_log.InfoFormat("Listening on {0}", address);
                    m_serverSocket.Bind(address);
                }
                m_serverSocket.ReceiveReady += OnMessage;

                m_poller = new NetMQPoller();
                m_poller.Add(m_serverSocket);

                m_poller.Run();
            }
        }

        public void Stop()
        {
            m_poller.Stop();
        }

        private void OnMessage(object sender, NetMQSocketEventArgs e)
        {
            byte[] messageTypeBytes = m_serverSocket.ReceiveFrameBytes();

            MessageType messageType = (MessageType)messageTypeBytes[0];

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
            byte[] transactionIdBytes = m_serverSocket.ReceiveFrameBytes();

            int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

            byte[] documentIdBytes = m_serverSocket.ReceiveFrameBytes();

            DocumentId documentId = new DocumentId(documentIdBytes);

            try
            {
                m_db.TransactionDelete(transactionId, documentId);

                // sending success
                m_serverSocket.SendFrame(Protocol.Success);
            }
            catch (TransactionNotExistException ex)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Transaction doesn't exist");
            }
            catch (DocumentLockedException)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Document locked by another transaction");
            }
        }

        private void TransactionUpdate()
        {
            byte[] transactionIdBytes = m_serverSocket.ReceiveFrameBytes();

            int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

            byte[] documentIdBytes = m_serverSocket.ReceiveFrameBytes();

            DocumentId documentId = new DocumentId(documentIdBytes);

            byte[] blob = m_serverSocket.ReceiveFrameBytes();

            try
            {
                m_db.TransactionUpdate(transactionId, documentId, blob);

                // sending success
                m_serverSocket.SendFrame(Protocol.Success);
            }
            catch (TransactionNotExistException ex)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Transaction doesn't exist");
            }
            catch (DocumentLockedException)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Document locked by another transaction");
            }
        }

        private void TransactionGet()
        {
            byte[] transactionIdBytes = m_serverSocket.ReceiveFrameBytes();

            int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

            byte[] documentIdBytes = m_serverSocket.ReceiveFrameBytes();

            DocumentId documentId = new DocumentId(documentIdBytes);

            try
            {
                byte[] blob = m_db.TransactionGet(transactionId, documentId);

                if (blob == null)
                {
                    blob = new byte[0];
                }

                m_serverSocket.SendMoreFrame(Protocol.Success).SendFrame(blob);
            }
            catch (TransactionNotExistException ex)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Transaction doesn't exist");
            }
        }

        private void Rollback()
        {
            byte[] transactionIdBytes = m_serverSocket.ReceiveFrameBytes();

            int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

            try
            {
                m_db.RollbackTransaction(transactionId);
                m_serverSocket.SendFrame(Protocol.Success);
            }
            catch (TransactionNotExistException ex)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Transaction doesn't exist");
            }
        }

        private void Commit()
        {
            byte[] transactionIdBytes = m_serverSocket.ReceiveFrameBytes();

            int transactionId = BitConverter.ToInt32(transactionIdBytes, 0);

            try
            {
                m_db.CommitTransaction(transactionId);
                m_serverSocket.SendFrame(Protocol.Success);
            }
            catch (TransactionNotExistException ex)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Transaction doesn't exist");
            }
        }

        private void StartTransaction()
        {
            int transactionId = m_db.StartTransaction();

            m_serverSocket.SendMoreFrame(Protocol.Success).SendFrame(BitConverter.GetBytes(transactionId));
        }

        private void Update()
        {
            byte[] documentIdBytes = m_serverSocket.ReceiveFrameBytes();

            DocumentId documentId = new DocumentId(documentIdBytes);

            byte[] blob = m_serverSocket.ReceiveFrameBytes();

            try
            {
                m_db.Update(documentId, blob);

                // sending success
                m_serverSocket.SendFrame(Protocol.Success);
            }
            catch (DocumentLockedException)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Document locked by another transaction");
            }
        }

        private void Delete()
        {
            byte[] documentIdBytes = m_serverSocket.ReceiveFrameBytes();

            DocumentId documentId = new DocumentId(documentIdBytes);

            try
            {
                m_db.Delete(documentId);

                // sending success
                m_serverSocket.SendFrame(Protocol.Success);
            }
            catch (DocumentLockedException)
            {
                m_serverSocket.SendMoreFrame(Protocol.Failed).SendFrame("Document locked by another transaction");
            }
        }

        private void Get()
        {
            byte[] documentIdBytes = m_serverSocket.ReceiveFrameBytes();

            DocumentId documentId = new DocumentId(documentIdBytes);

            byte[] blob = m_db.Get(documentId);

            if (blob == null)
            {
                blob = new byte[0];
            }

            m_serverSocket.SendMoreFrame(Protocol.Success).SendFrame(blob);
        }
    }
}