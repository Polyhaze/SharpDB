namespace SomDB.Engine.Domain
{
	public class DocumentRevision
	{
		public DocumentRevision(DocumentId documentId, ulong timeStamp, long blobFileLocation, int blobSize)
		{
			DocumentId = documentId;
			TimeStamp = timeStamp;
			BlobFileLocation = blobFileLocation;
			BlobSize = blobSize;

			ExpireTimeStamp = 0;
		}

		public DocumentId DocumentId { get; private set; }
		
		public ulong TimeStamp { get; private set; }

		public ulong ExpireTimeStamp { get; private set; }

		public long BlobFileLocation { get; private set; }

		public int BlobSize { get; private set; }

		public bool IsDeleted {get { return BlobSize == 0; }}

		public void Expire(ulong timestamp)
		{
			ExpireTimeStamp = timestamp;
		}
	}
}
