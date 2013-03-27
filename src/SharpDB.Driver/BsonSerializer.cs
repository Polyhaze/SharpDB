using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace SharpDB.Driver
{
	public class BsonSerializer : ISerializer
	{
		private JsonSerializer m_serializer;
		private MemoryStream m_memoryStream;
		private BsonWriter m_bsonWriter;
		private BinaryWriter m_binaryWriter;

		public const byte NumberType = 0;		
		public const byte StringType = 1;
		public const byte CustomType = 2;

		public BsonSerializer()
		{
			m_serializer = new JsonSerializer();
			m_memoryStream = new MemoryStream(1024 * 1024);
			m_bsonWriter = new BsonWriter(m_memoryStream);
			m_binaryWriter = new BinaryWriter(m_memoryStream, Encoding.Unicode);
		}

		public virtual byte[] SerializeDocumentId(object documentId)
		{
			m_memoryStream.Position = 0;

			if (documentId is int)
			{
				// serializing the type of the document id in order to avoid case of 4 bytes string equal to int
				m_binaryWriter.Write(NumberType);
				m_binaryWriter.Write((int)documentId);
			}
			else if (documentId is long)
			{
				// serializing the type of the document id in order to avoid case of 8 bytes string equal to long
				m_binaryWriter.Write(NumberType);				

				long number = (long) documentId;

				// to save space, if the number is less than the size of int we save int instead of long
				if (number < int.MaxValue)
				{
					m_binaryWriter.Write((int)number);
				}
				else
				{
					m_binaryWriter.Write(number);					
				}
			}
			else if (documentId is string)
			{
				m_binaryWriter.Write(StringType);
				m_binaryWriter.Write((string)documentId);
			}
			else
			{
				m_binaryWriter.Write(CustomType);
				m_serializer.Serialize(m_bsonWriter, documentId);
			}

			byte[] buffer = new byte[m_memoryStream.Position];
			Buffer.BlockCopy(m_memoryStream.GetBuffer(), 0, buffer, 0, (int)m_memoryStream.Position);

			return buffer;
		}

		public virtual byte[] SerializeDocument<T>(T document)
		{
			m_memoryStream.Position = 0;
			m_serializer.Serialize(m_bsonWriter, document);

			byte[] blob = new byte[m_memoryStream.Position];
			Buffer.BlockCopy(m_memoryStream.GetBuffer(), 0, blob, 0, (int)m_memoryStream.Position);

			return blob;
		}

		public virtual T DeserializeDocument<T>(byte[] bytes)
		{
			using (var stream = new MemoryStream(bytes))
			{
				using (BsonReader reader = new BsonReader(stream))
				{
					return m_serializer.Deserialize<T>(reader);
				}
			}
		}

		public virtual Newtonsoft.Json.Linq.JObject DeserializeToJObject(byte[] bytes)
		{
			using (var stream = new MemoryStream(bytes))
			{
				using (BsonReader reader = new BsonReader(stream))
				{
					return Newtonsoft.Json.Linq.JObject.Load(reader);
				}
			}			
		}

		public virtual byte[] SerializeFronJObject(Newtonsoft.Json.Linq.JObject jobject)
		{
			m_memoryStream.Position = 0;
			
			jobject.WriteTo(m_bsonWriter);

			byte[] blob = new byte[m_memoryStream.Position];
			Buffer.BlockCopy(m_memoryStream.GetBuffer(), 0, blob, 0, (int)m_memoryStream.Position);

			return blob;		
		}

		public virtual void Dispose()
		{
			m_memoryStream.Dispose();

			m_serializer = null;
			m_memoryStream = null;
			m_bsonWriter = null;
			m_binaryWriter = null;
		}

	}
}
