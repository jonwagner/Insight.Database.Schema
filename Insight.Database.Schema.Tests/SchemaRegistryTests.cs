using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using System.Transactions;
using System.Configuration;
using System.Data.SqlClient;
using Insight.Database.Schema;
using System.Data.Common;
using Moq;

namespace Insight.Database.Schema.Tests
{
	/// <summary>
	/// Test the SchemaRegistry class.
	/// </summary>
	[TestFixture]
	public class SchemaRegistryTests : BaseInstallerTest
	{
		[Test]
		public void SchemaRegistryIsCreatedAutomatically([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithRollback(connectionString, connection =>
			{
				// create the registry
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);

				// make sure the table exists
				Assert.AreEqual(1, connection.ExecuteScalarSql<int>("SELECT COUNT(*) FROM sys.objects WHERE name = @Name", new Dictionary<string, object> () { { "Name", SchemaRegistry.SchemaRegistryTableName } }));

				// make sure the entries are empty
				Assert.AreEqual(0, registry.Entries.Count);

				// create another registry to make sure that it doesn't blow up
				registry = new SchemaRegistry(connection, TestSchemaGroup);
			});
		}

		[Test]
		public void SchemaRegistryCanAddNewRecords([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithRollback(connectionString, connection =>
			{
				// create the registry
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);

				// make sure the entries are empty
				Assert.AreEqual(0, registry.Entries.Count);

				// add an entry and save it to the database
				registry.Entries.Add(new SchemaRegistryEntry()
				{
					SchemaGroup = "test",
					ObjectName = "Beer",
					Type = SchemaObjectType.Table,
					Signature = "1234",
					OriginalOrder = 1
				});
				registry.Commit();

				// create another registry and make sure it loads the entries
				registry = new SchemaRegistry(connection, TestSchemaGroup);
				Assert.AreEqual(1, registry.Entries.Count);

				// add an second entry and save it to the database
				registry.Entries.Add(new SchemaRegistryEntry()
				{
					SchemaGroup = "test",
					ObjectName = "Beer2",
					Type = SchemaObjectType.Table,
					Signature = "1234",
					OriginalOrder = 2
				});
				registry.Commit();

				// create another registry and make sure it loads the entries
				registry = new SchemaRegistry(connection, TestSchemaGroup);
				Assert.AreEqual(2, registry.Entries.Count);
			});
		}

		[Test]
		public void SchemaRegistryCanRemoveOldRecords([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithRollback(connectionString, connection =>
			{
				// create the registry
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);

				// add an entry and save it to the database
				registry.Entries.Add(new SchemaRegistryEntry()
				{
					SchemaGroup = "test",
					ObjectName = "Beer",
					Type = SchemaObjectType.Table,
					Signature = "1234",
					OriginalOrder = 1
				});
				registry.Commit();

				// create another registry and make sure it loads the entries
				registry = new SchemaRegistry(connection, TestSchemaGroup);
				Assert.AreEqual(1, registry.Entries.Count);

				// clear the entries
				registry.Entries.Clear();
				registry.Commit();

				// create another registry and make sure it loads the entries
				registry = new SchemaRegistry(connection, TestSchemaGroup);
				Assert.AreEqual(0, registry.Entries.Count);
			});
		}

		[Test]
		public void SchemaRegistryShouldScriptCreateTableAndSelect([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithRollback(connectionString, connection =>
			{
				// create the registry
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);

				// we want to script the create table for the registry
				// we also need to execute it all the time otherwise the rest of the process blows up
				Assert.IsTrue(connection.ScriptLog.ToString().Contains(String.Format("CREATE TABLE [{0}]", SchemaRegistry.SchemaRegistryTableName)));
				Assert.IsTrue(connection.ExecutionLog.ToString().Contains(String.Format("CREATE TABLE [{0}]", SchemaRegistry.SchemaRegistryTableName)));

				// we don't want to script the select of the registry
				Assert.IsFalse(connection.ScriptLog.ToString().Contains(String.Format("SELECT * FROM [{0}]", SchemaRegistry.SchemaRegistryTableName)));
				Assert.IsTrue(connection.ExecutionLog.ToString().Contains(String.Format("SELECT * FROM [{0}]", SchemaRegistry.SchemaRegistryTableName)));
			});
		}

		[Test]
		public void SchemaRegistryShouldNotExecuteSchemaUpdateInRecordOnlyMode([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithRollback(connectionString, connection =>
			{
				// only script the data
				connection.ScriptOnly = true;

				// create the registry
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);
				registry.Entries.Add(new SchemaRegistryEntry()
				{
					SchemaGroup = "test",
					ObjectName = "Beer",
					Type = SchemaObjectType.Table,
					Signature = "1234",
					OriginalOrder = 1
				});
				registry.Commit();

				// we want to script the delete or insert into the registry, but not execute them in script mode
				Assert.IsTrue(connection.ScriptLog.ToString().Contains(String.Format("DELETE FROM [{0}]", SchemaRegistry.SchemaRegistryTableName)));
				Assert.IsTrue(connection.ScriptLog.ToString().Contains(String.Format("INSERT INTO [{0}]", SchemaRegistry.SchemaRegistryTableName)));
				Assert.IsFalse(connection.ExecutionLog.ToString().Contains(String.Format("DELETE FROM [{0}]", SchemaRegistry.SchemaRegistryTableName)));
				Assert.IsFalse(connection.ExecutionLog.ToString().Contains(String.Format("INSERT INTO [{0}]", SchemaRegistry.SchemaRegistryTableName)));
			});
		}
	}
}
