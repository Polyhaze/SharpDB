using System;

namespace SomDB.Driver
{
	public interface ISerializer : IDisposable
	{
		byte[] SerializeDocumentId(object documentId);
		byte[] SerializeDocument<T>(T document);
		T DeserializeDocument<T>(byte[] bytes);
	}
}