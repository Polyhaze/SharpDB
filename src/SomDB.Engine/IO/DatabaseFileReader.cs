using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using SomDB.Engine.Domain;

namespace SomDB.Engine.IO
{
	public class DatabaseFileReader : IDisposable
	{
		private FileStream m_readStream;

		public DatabaseFileReader(string fileName)
		{
			FileName = fileName;

			m_readStream = new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

		public string FileName { get; private set; }

		public Dictionary<string, Document> GetDocuments(out ulong dbTimestamp)
		{
			dbTimestamp = 0;

			Dictionary<string, Document> documents = new Dictionary<string, Document>();

			// initialize the buffers
			byte[] timestampBuffer = new byte[12];
			byte[] objectIdLengthBuffer = new byte[2];
			byte[] objectIdBuffer = new byte[UInt16.MaxValue];

			byte[] blobLengthBuffer = new byte[4];

			int numberOfDocuments = 0;
			int documentsCounter = 0;

			// now we read all the objects meta data (not loading any data yet)
			while (m_readStream.Position < m_readStream.Length)
			{
				if (numberOfDocuments == documentsCounter)
				{
					// now we are reading the object timestamp
					m_readStream.Read(timestampBuffer, 0, 12);
					dbTimestamp = BitConverter.ToUInt64(timestampBuffer, 0);

					numberOfDocuments = BitConverter.ToInt32(timestampBuffer, 8);
					documentsCounter = 0;
				}

				// first is the object id lenth
				m_readStream.Read(objectIdLengthBuffer, 0, 2);
				UInt16 objectIdLength = BitConverter.ToUInt16(objectIdLengthBuffer, 0);

				// read the objectId
				m_readStream.Read(objectIdBuffer, 0, objectIdLength);
				string objectId = Encoding.ASCII.GetString(objectIdBuffer, 0, objectIdLength);

				// read the blob length
				m_readStream.Read(blobLengthBuffer, 0, 4);
				int blobLength = BitConverter.ToInt32(blobLengthBuffer, 0);
				long blobLocation = m_readStream.Position;

				// take the position of the file to the next document
				m_readStream.Position += blobLength;

				if (!documents.ContainsKey(objectId))
				{
					documents.Add(objectId, new Document(objectId, dbTimestamp, blobLocation, blobLength));
				}
				else
				{
					Document metaData = documents[objectId];

					metaData.Update(dbTimestamp, blobLocation, blobLength, false);
				}

				documentsCounter++;
			}

			if (documentsCounter != numberOfDocuments)
			{
				// database is corrupted, need to recover
				throw new Exception("Database is corrupted");
			}

			return documents;
		}

		public byte[] ReadDocument(long fileLocation, int size)
		{
			m_readStream.Position = fileLocation;

			byte[] blob = new byte[size];
			m_readStream.Read(blob, 0, size);

			return blob;
		}


		public void Dispose()
		{
			m_readStream.Dispose();

			m_readStream = null;
		}
	}
}
