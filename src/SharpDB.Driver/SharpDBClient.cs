using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using NetMQ;

namespace SharpDB.Driver
{
    public class SharpDBClient : IDisposable
    {
        private bool m_isDisposed = false;

        private ConcurrentBag<SharpDBConnection> m_connections = new ConcurrentBag<SharpDBConnection>();

        public SharpDBClient(string connectionString)
        {
            ConnectionString = connectionString;
            SerializerFactory = () => new BsonSerializer();
        }

        public Func<ISerializer> SerializerFactory { get; set; }

        public string ConnectionString { get; private set; }

        public SharpDBConnection GetConnection()
        {
            NetMQSocket socket = new NetMQ.Sockets.RequestSocket();
            //	socket.Options.CopyMessages = false;
            socket.Options.Linger = TimeSpan.FromSeconds(5);
            socket.Connect(ConnectionString);

            var connection = new SharpDBConnection(this, socket, SerializerFactory());
            m_connections.Add(connection);

            return connection;
        }

        internal void ReleaseConnection(SharpDBConnection connection)
        {
            connection.Socket.Dispose();
            connection.Serializer.Dispose();
        }

        public void Dispose()
        {
            lock (this)
            {
                if (!m_isDisposed)
                {
                    SharpDBConnection connection;
                    while (m_connections.TryTake(out connection))
                    {
                        connection.Dispose();
                    }
                    m_connections = null;
                    m_isDisposed = true;
                }
            }
        }
    }
}