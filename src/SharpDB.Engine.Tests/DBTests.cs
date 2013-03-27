using System.IO;
using NUnit.Framework;
using SharpDB.Engine.Domain;
using SharpDB.Engine.IO;
using SharpDB.Engine.Cache;

namespace SharpDB.Engine.Tests
{
	[TestFixture]
	public class DBTests
	{
		private KeyValueDatabase m_db;

		[SetUp]
		public void SetUpTest()
		{
			m_db = new KeyValueDatabase(filename => new DatabaseFileReader(filename), filename=> new DatabaseFileWriter(filename),
				filename => new MemoryCacheProvider(filename));
			
			m_db.Start();
		}

		[TearDown]
		public void TearDownTest()
		{
			m_db.Stop();
			m_db = null;

			File.Delete("test.dbfile");
		}

		[Test]
		public void TryReadFutureDocument()
		{
			int transactionId = m_db.StartTransaction();
 
			DocumentId id1 = new DocumentId("1");

			// this object is stored out of transaction therefore should be visible for the transaction
			m_db.Update(id1, new byte[1] { 0 });

			byte[] blob = m_db.TransactionGet(transactionId, id1);

			Assert.IsNull(blob);

			m_db.CommitTransaction(transactionId);
		}

		[Test]
		public void ReadOldRevisionDuringTransaction()
		{
			DocumentId id1 = new DocumentId("1");

			m_db.Update(id1, new byte[1] { 0 });

			int transactionId = m_db.StartTransaction();

			m_db.Update(id1, new byte[1] { 1 });

			byte[] blob = m_db.TransactionGet(transactionId, id1);

			Assert.AreEqual(0, blob[0]);

			m_db.CommitTransaction(transactionId);

			transactionId = m_db.StartTransaction();

			blob = m_db.TransactionGet(transactionId, id1);

			Assert.AreEqual(1, blob[0]);

			m_db.CommitTransaction(transactionId);
		}

		[Test]
		public void ReadDocumentUpdatedByTransaction()
		{
			DocumentId id1 = new DocumentId("1");

			m_db.Update(id1, new byte[1] { 0 });

			int transactionId = m_db.StartTransaction();

			m_db.TransactionUpdate(transactionId, id1, new byte[1] { 1 });

			byte[] blob = m_db.TransactionGet(transactionId, id1);
			Assert.AreEqual(1, blob[0]);

			// reading without transaction should return uncommitted document
			blob = m_db.Get(id1);
			Assert.AreEqual(0, blob[0]);

			m_db.CommitTransaction(transactionId);

			// reading the object agian after the transaction committed making sure we are reading the new committed document
			blob = m_db.Get(id1);
			Assert.AreEqual(1, blob[0]);
		}
	}
}
