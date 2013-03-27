using System;
using System.IO;
using System.Text;
using SharpDB.Engine.Domain;

namespace SharpDB.Engine.IO
{
	public class DatabaseFileWriter : IDatabaseWriter
	{
		private FileStream m_writerStream;		

		private byte[] m_writeDocumentBuffer;

		private byte[] m_writeStartTimestampBuffer;

		private const uint BlobMaxSize = 1024 * 1024; // One mega

		public DatabaseFileWriter(string fileName)
		{
			FileName = fileName;

			m_writerStream = new FileStream(FileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
			m_writerStream.Position = m_writerStream.Length;

			// calculating the max size of an object
			uint size = 2 + UInt16.MaxValue + 4 + BlobMaxSize;

			m_writeDocumentBuffer = new byte[size];

			// timestamp size + int16 size
			m_writeStartTimestampBuffer = new byte[12];
		}

		public string FileName { get; private set; }

		public long WriteDocument(DocumentId documentId, byte[] blob)
		{
			// calculate the size of the document including meta data
			int size = 2 + documentId.Length + 4 + blob.Length;

			int position = 0;

			Buffer.BlockCopy(BitConverter.GetBytes((UInt16)documentId.Length), 0, m_writeDocumentBuffer, position, 2);
			position += 2;

			Buffer.BlockCopy(documentId.Bytes, 0, m_writeDocumentBuffer, position, documentId.Length);
			position += documentId.Length;

			Buffer.BlockCopy(BitConverter.GetBytes(blob.Length), 0, m_writeDocumentBuffer, position, 4);
			position += 4;

			if (blob.Length > 0)
			{
				Buffer.BlockCopy(blob, 0, m_writeDocumentBuffer, position, blob.Length);
			}

			m_writerStream.Write(m_writeDocumentBuffer, 0, size);

			return m_writerStream.Position - blob.Length;
		}
		
		public void BeginTimestamp(ulong timestamp, int numberOfDocuments)
		{
			Buffer.BlockCopy(BitConverter.GetBytes(timestamp), 0, m_writeStartTimestampBuffer, 0, 8);
			Buffer.BlockCopy(BitConverter.GetBytes(numberOfDocuments), 0, m_writeStartTimestampBuffer, 8, 4);

			m_writerStream.Write(m_writeStartTimestampBuffer, 0, 12);
		}

		public void Flush()
		{
			m_writerStream.Flush();
		}


		public void Dispose()
		{
			m_writerStream.Dispose();
			
			m_writerStream = null;
			m_writeDocumentBuffer = null;
			m_writeStartTimestampBuffer = null;
		}
	}
}
