using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Insight.Database;
using System.Data.SqlClient;

namespace Insight.Database.Schema.Tests
{
	[TestFixture]
	public class UseAutoProcTests : BaseInstallerTest
	{
		public class Beer
		{
			public int ID;
			public string Name;
		}

		[Test]
		public void InsertSetsID()
		{
			Test(c =>
			{
				var beer = new Beer() { Name = "Yum" };
				c.Insert("InsertBeer", beer);

				Assert.AreEqual(1, beer.ID);
			});
		}

		/// <summary>
		/// Validate that a large set of objects gets inserted with ids returned properly.
		/// </summary>
		[Test]
		public void InsertManySetsIDsProperly()
		{
			Test(c =>
			{
				var list = new List<Beer>();
				for (int i = 0; i < 20000; i++)
					list.Add(new Beer() { Name = i.ToString() });

				c.InsertList("InsertBeers", list);

				for (int i = 0; i < list.Count; i++)
				{
					Assert.AreEqual(i.ToString(), c.ExecuteScalarSql<string>("SELECT Name FROM Beer WHERE ID=@id", list[i]));
				}
			});
		}

		/// <summary>
		/// Validate that a large set of objects gets inserted with ids returned properly.
		/// </summary>
		[Test]
		public void UpsertManySetsIDsProperly()
		{
			Test(c =>
			{
				var list = new List<Beer>();
				for (int i = 0; i < 20000; i++)
					list.Add(new Beer() { Name = i.ToString() });

				c.InsertList("UpsertBeers", list);

				for (int i = 0; i < list.Count; i++)
				{
					Assert.AreEqual(i.ToString(), c.ExecuteScalarSql<string>("SELECT Name FROM Beer WHERE ID=@id", list[i]));
				}
			});
		}

		private void Test(Action<SqlConnection> action)
		{
			var connectionString = base.ConnectionStrings.First();

			TestWithDrop(connectionString, () =>
			{
				if (!SchemaInstaller.DatabaseExists(connectionString))
					SchemaInstaller.CreateDatabase(connectionString);

				using (var c = new SqlConnection(connectionString))
				{
					c.Open();
					SetUpDatabase(c);
					action(c);
				}
			});
		}

		private void SetUpDatabase(SqlConnection connection)
		{
			SchemaInstaller installer = new SchemaInstaller(connection);
			SchemaObjectCollection schema = new SchemaObjectCollection();
			schema.Add("CREATE TABLE Beer ([id] [int] NOT NULL IDENTITY, [name] varchar(100))");
			schema.Add("ALTER TABLE Beer ADD CONSTRAINT PK_Beer PRIMARY KEY ([ID])");
			schema.Add("-- AUTOPROC All Beer");
			installer.Install("default", schema);
		}
	}
}
