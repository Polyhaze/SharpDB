using System;
using SharpDB.Engine.Domain;

namespace SharpDB.Engine.IO
{
	public interface IDatabaseWriter : IDisposable
	{
		string FileName { get; }
		long WriteDocument(DocumentId documentId, byte[] blob);
		void BeginTimestamp(ulong timestamp, int numberOfDocuments);
		void Flush();
	}
}