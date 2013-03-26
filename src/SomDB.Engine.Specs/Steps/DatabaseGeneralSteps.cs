using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TechTalk.SpecFlow;

namespace SomDB.Engine.Specs.Steps
{
	[Binding]
	public class DatabaseGeneralSteps
	{
		private readonly DatabaseContext m_databaseContext;

		public DatabaseGeneralSteps(DatabaseContext databaseContext)
		{
			m_databaseContext = databaseContext;
		}


		[StepDefinition(@"Restrt the database")]
		public void WhenRestrtTheDatabase()
		{
			m_databaseContext.Restart();
		}

	}
}
