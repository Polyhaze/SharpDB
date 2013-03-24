using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SomDB.Engine;

namespace SomDB.Tests
{
	[TestFixture]
	public class DBTests
	{
		private DB m_db;

		[SetUp]
		public void SetUpTest()
		{
			m_db = new DB("test.dbfile");
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
 
			// this object is stored out of transaction therefore should be visible for the transaction
			m_db.Store("1", new byte[1] { 0 });

			byte[] blob = m_db.TransactionRead(transactionId, "1");

			Assert.IsNull(blob);

			m_db.CommitTransaction(transactionId);
		}

		[Test]
		public void ReadOldRevisionDuringTransaction()
		{
			m_db.Store("1", new byte[1] { 0 });

			int transactionId = m_db.StartTransaction();

			m_db.Store("1", new byte[1] { 1 });

			byte[] blob = m_db.TransactionRead(transactionId, "1");

			Assert.AreEqual(0, blob[0]);

			m_db.CommitTransaction(transactionId);

			transactionId = m_db.StartTransaction();

			blob = m_db.TransactionRead(transactionId, "1");

			Assert.AreEqual(1, blob[0]);

			m_db.CommitTransaction(transactionId);
		}

		[Test]
		public void ReadDocumentUpdatedByTransaction()
		{
			m_db.Store("1", new byte[1] { 0 });

			int transactionId = m_db.StartTransaction();

			m_db.TransactionUpdate(transactionId, "1", new byte[1] { 1 });

			byte[] blob = m_db.TransactionRead(transactionId, "1");
			Assert.AreEqual(1, blob[0]);

			// reading without transaction should return uncommitted document
			blob = m_db.Read("1");
			Assert.AreEqual(0, blob[0]);

			m_db.CommitTransaction(transactionId);

			// reading the object agian after the transaction committed making sure we are reading the new committed document
			blob = m_db.Read("1");
			Assert.AreEqual(1, blob[0]);
		}
	}
}
