using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SharpDB.Engine.Cache;
using SharpDB.Engine.Domain;
using SharpDB.Engine.IO;
using TechTalk.SpecFlow;

namespace SharpDB.Engine.Specs.Steps
{
	[Binding]
	public class GetUpdateDeleteSteps
	{
		private readonly DatabaseContext m_databaseContext;

		private byte[] m_documentValue;

		public GetUpdateDeleteSteps(DatabaseContext databaseContext)
		{
			m_databaseContext = databaseContext;
		}

		[Given(@"Document Key=(.*) and Value=""(.*)"" in the DB")]
		[When(@"I update a document with Key=(.*) and Value=""(.*)""")]
		public void UpdateDocument(int key, string value)
		{
			byte[] valueBytes = Encoding.ASCII.GetBytes(value);

			m_databaseContext.Database.Update(new DocumentId(key), valueBytes);
		}

		[Then(@"Document is in the database with Key=(.*) and Value=""(.*)""")]
		public void ThenDocumentIsInTheDatabaseWithKeyAndValue(int key, string value)
		{
			DocumentId documentId = new DocumentId(key);

			byte[] valueBytes = m_databaseContext.Database.Get(documentId);

			string dbValue = Encoding.ASCII.GetString(valueBytes);

			Assert.AreEqual(dbValue, value);
		}		

		[When(@"I get document with key=(.*)")]
		public void WhenIGetDocumentWithKey(int key)
		{
			m_documentValue = m_databaseContext.Database.Get(new DocumentId(key));
		}

		[Then(@"Document should exist and Value=""(.*)""")]
		public void ThenDocumentShouldExistAndValue(string value)
		{
			Assert.Greater(value.Length, 0);

			string dbValue = Encoding.ASCII.GetString(m_documentValue);

			Assert.AreEqual(dbValue, value);
		}

		[When(@"I delete document with key=(.*)")]
		public void WhenIDeleteDocumentWithKey(int key)
		{
			m_databaseContext.Database.Delete(new DocumentId(Convert.ToInt32(key)));
		}

		[Then(@"Document with key=(.*) should not exist")]
		public void ThenDocumentWithKeyShouldNotExist(int key)
		{
			byte[] valueBytes = m_databaseContext.Database.Get(new DocumentId(key));

			Assert.AreEqual(0, valueBytes.Length);
		}


	}
}
