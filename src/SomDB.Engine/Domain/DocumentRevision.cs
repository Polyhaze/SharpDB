namespace SomDB.Engine.Domain
{
	public class DocumentRevision
	{
		public DocumentRevision(string documentId, ulong timeStamp, long blobFileLocation, int blobSize)
		{
			DocumentId = documentId;
			TimeStamp = timeStamp;
			BlobFileLocation = blobFileLocation;
			BlobSize = blobSize;

			ExpireTimeStamp = 0;
		}

		public string DocumentId { get; private set; }
		
		public ulong TimeStamp { get; private set; }

		public ulong ExpireTimeStamp { get; private set; }

		public long BlobFileLocation { get; private set; }

		public int BlobSize { get; private set; }

		public void Expire(ulong timestamp)
		{
			ExpireTimeStamp = timestamp;
		}
	}
}
