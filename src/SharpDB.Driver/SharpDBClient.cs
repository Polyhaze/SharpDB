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
		private NetMQContext m_context;
		private readonly bool m_contextOwned;
		private bool m_isDisposed = false;

		ConcurrentBag<SharpDBConnection> m_connections = new ConcurrentBag<SharpDBConnection>();

		public SharpDBClient(NetMQContext context, string connectionString)
		{
			m_context = context;
			m_contextOwned = false;
			ConnectionString = connectionString;
			SerializerFactory = () => new BsonSerializer();
		}

		public SharpDBClient(string connectionString)
		{
			m_context = NetMQContext.Create();
			m_contextOwned = true;
			ConnectionString = connectionString;
			SerializerFactory = () => new BsonSerializer();
		}

		public Func<ISerializer> SerializerFactory { get; set; } 

		public string ConnectionString { get; private set; }

		public SharpDBConnection GetConnection()
		{
			NetMQSocket socket = m_context.CreateRequestSocket();
			socket.Options.CopyMessages = false;
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

					if (m_contextOwned)
					{
						m_context.Dispose();
					}

					m_connections = null;
					m_context = null;

					m_isDisposed = true;
				}
			}
		}


	}
}
