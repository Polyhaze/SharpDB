using System.Collections.Generic;
using System.IO;
using System.Linq;
using SomDB.Engine.Domain;
using SomDB.Engine.IO;

namespace SomDB.Engine.Backup
{
	public class FullBackup
	{
		private readonly IList<DocumentRevision> m_documents;
		
		public FullBackup(IList<DocumentRevision> documents, string sourceFileName, string destinationFileName)
		{
			m_documents = documents;
			SourceFileName = sourceFileName;
			DestinationFileName = destinationFileName;
		}

		public string SourceFileName { get; private set; }
		public string DestinationFileName { get; private set; }
	
		public void Start()
		{
			File.Delete(DestinationFileName);

			using (DatabaseFileReader reader = new DatabaseFileReader(SourceFileName))
			using (DatabaseFileWriter writer =new DatabaseFileWriter(DestinationFileName))
			{
				var documentsByTimestamp = m_documents.GroupBy(d => d.TimeStamp).OrderBy(g=> g.Key);

				foreach (IGrouping<ulong, DocumentRevision> timestampRevisions in documentsByTimestamp)
				{
					writer.BeginTimestamp(timestampRevisions.Key, timestampRevisions.Count());

					foreach (DocumentRevision documentRevision in timestampRevisions)
					{
						byte[] blob = reader.ReadDocument(documentRevision.BlobFileLocation, documentRevision.BlobSize);

						writer.WriteDocument(documentRevision.DocumentId, blob);
					}

					writer.Flush();
				}
			}		
		}
	}
}
