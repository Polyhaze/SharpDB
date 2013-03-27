using System;
using System.Collections.Generic;
using SharpDB.Engine.Domain;

namespace SharpDB.Engine.IO
{
	public interface IDatabaseReader : IDisposable
	{
		string FileName { get; }
		Dictionary<DocumentId, Document> GetDocuments(out ulong dbTimestamp);
		byte[] ReadDocument(long fileLocation, int size);
	}
}