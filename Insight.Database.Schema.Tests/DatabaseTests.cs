using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using System.Transactions;
using System.Configuration;
using System.Data.SqlClient;

namespace Insight.Database.Schema.Tests
{
	/// <summary>
	/// Test create/drop database.
	/// </summary>
	[TestFixture]
	public class DatabaseTests : BaseInstallerTest
	{
		#region DatabaseExists Tests
		[Test]
		public void DatabaseExistsReturnsTrueForMaster([ValueSource("ConnectionStrings")] string connectionString)
		{
			SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
			builder.InitialCatalog = "master";

			Assert.True(SchemaInstaller.DatabaseExists(builder.ConnectionString));
		}

		[Test]
		public void DatabaseExistsReturnsTrueForInvalid([ValueSource("ConnectionStrings")] string connectionString)
		{
			SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
			builder.InitialCatalog = "fhjasjkl";

			Assert.False(SchemaInstaller.DatabaseExists(builder.ConnectionString));
		}
		#endregion

		#region Create Tests
		[Test]
		public void TestCreateDatabase([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithDrop(connectionString, () =>
			{
				// drop the database if it already exists
				if (SchemaInstaller.DatabaseExists(connectionString))
					SchemaInstaller.DropDatabase(connectionString);

				// create the database
				Assert.True(SchemaInstaller.CreateDatabase(connectionString));

				// make sure the database exises
				Assert.True(SchemaInstaller.DatabaseExists(connectionString));

				// create the database again, it should return false
				Assert.False(SchemaInstaller.CreateDatabase(connectionString));
			});
		}
		#endregion

		#region Drop Tests
		[Test]
		public void TestDropDatabase([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithDrop(connectionString, () =>
			{
				// create the database if it doesn't exist
				if (!SchemaInstaller.DatabaseExists(connectionString))
					SchemaInstaller.CreateDatabase(connectionString);

				// drop the database
				Assert.True(SchemaInstaller.DropDatabase(connectionString));

				// make sure the database doesn't exist
				Assert.False(SchemaInstaller.DatabaseExists(connectionString));

				// drop the database again, it should return false
				Assert.False(SchemaInstaller.DropDatabase(connectionString));
			});
		}
		#endregion
	}
}
