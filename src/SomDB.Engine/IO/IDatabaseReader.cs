using System;
using System.Collections.Generic;
using SomDB.Engine.Domain;

namespace SomDB.Engine.IO
{
	public interface IDatabaseReader : IDisposable
	{
		string FileName { get; }
		Dictionary<DocumentId, Document> GetDocuments(out ulong dbTimestamp);
		byte[] ReadDocument(long fileLocation, int size);
	}
}