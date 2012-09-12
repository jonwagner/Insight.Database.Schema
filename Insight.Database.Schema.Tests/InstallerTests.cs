using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using System.Transactions;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Data.Common;

namespace Insight.Database.Schema.Tests
{
	[TestFixture]
	public class InstallerTests : BaseInstallerTest
	{
		/// <summary>
		/// The schemas to use for testing.
		/// NOTE: if you create dependencies between items, put them in dependency order.
		/// The Drop test case (and probably others) will use this order to help execute test cases.
		/// </summary>
		public IEnumerable<IEnumerable<string>> Schemas = new List<IEnumerable<string>>()
		{
			// just tables
			new string[] 
			{ 
				@"CREATE TABLE Beer ([ID] [int], Description [varchar](128))",
				@"CREATE TABLE Wine ([ID] [int], Description [varchar](128))",
			},
			// just procs
			new string[] 
			{ 
				@"CREATE PROC TestProc1 AS SELECT 1",
				@"CREATE PROC TestProc2 AS SELECT 2",
			},
			// tables and procs
			new string[] 
			{ 
				@"CREATE TABLE [Beer] ([ID] [int], Description [varchar](128))",
				@"CREATE PROC [BeerProc] AS SELECT * FROM [Beer]",
			},
			// procs with dependencies on other procs and permissions
			new string[] 
			{ 
				@"CREATE PROC TestProc1 AS SELECT 1",
				@"CREATE PROC TestProc2 AS EXEC TestProc1",
				@"GRANT EXEC ON [TestProc1] TO [public]",
			},
			// just tables
			new string[] 
			{ 
				@"CREATE TABLE Beer ([ID] [int], Description [varchar](128))",
				@"GRANT SELECT ON [Beer] TO [public]",
				@"GRANT UPDATE ON [Beer] TO [public]",
			},
			// set of all supported dependencies based on a table
			new string[] 
			{ 
				@"CREATE TABLE Beer ([ID] [int] NOT NULL, Description [varchar](128))",
				@"ALTER TABLE [Beer] ADD CONSTRAINT PK_Beer PRIMARY KEY ([ID])",
				@"ALTER TABLE [Beer] ADD CONSTRAINT CK_BeerTable CHECK (ID > 0 OR Description > 'a')",
				@"ALTER TABLE [Beer] ADD CONSTRAINT CK_BeerColumn CHECK (ID > 5)",
				@"ALTER TABLE [Beer] ADD CONSTRAINT DF_Beer_Description DEFAULT 'IPA' FOR Description",

				@"CREATE VIEW BeerView AS SELECT * FROM Beer",
				@"CREATE PROC [BeerProc] AS SELECT * FROM BeerView",
				@"GRANT EXEC ON [BeerProc] TO [public]",

				@"CREATE FUNCTION [BeerFunc] () RETURNS [int] AS BEGIN DECLARE @i [int] SELECT @i=MAX(ID) FROM BeerView RETURN @i END",
				@"CREATE FUNCTION [BeerTableFunc] () RETURNS @IDs TABLE (ID [int]) AS BEGIN INSERT INTO @IDs SELECT ID FROM BeerView RETURN END",

				@"CREATE TABLE Keg ([ID] [int], [BeerID] [int])",
				@"ALTER TABLE [Keg] ADD CONSTRAINT FK_Keg_Beer FOREIGN KEY ([BeerID]) REFERENCES Beer (ID) ON DELETE SET NULL ON UPDATE CASCADE",

				@"-- AUTOPROC All [Beer]",
			},
			// set of dependencies based on user-defined types
			new string[]
			{
				@"CREATE TYPE BeerName FROM [varchar](256)",
				@"CREATE PROC BeerProc (@Name [BeerName]) AS SELECT @Name",
			},
			// tests around indexes
			new string[]
			{
				@"CREATE TABLE Beer ([ID] [int] NOT NULL, Description [varchar](128))",
				@"ALTER TABLE [Beer] ADD CONSTRAINT PK_Beer PRIMARY KEY NONCLUSTERED ([ID])",
				@"CREATE CLUSTERED INDEX [IX_Beer_Description] ON Beer (Description)",
			},
			// xml indexes
			new string[]
			{
				@"CREATE TABLE Beer ([ID] [int] NOT NULL, Description [xml])",
				@"ALTER TABLE [Beer] ADD CONSTRAINT PK_Beer PRIMARY KEY CLUSTERED ([ID])",
				@"CREATE PRIMARY XML INDEX IX_Beer_XML ON Beer (Description)",
				@"CREATE XML INDEX IX_Beer_Xml2 ON Beer(Description) USING XML INDEX IX_Beer_Xml FOR PATH",
			},
			// persistent views
			new string[]
			{
				@"CREATE TABLE Beer ([ID] [int] NOT NULL, Description [xml])",
				@"-- INDEXEDVIEW
					CREATE VIEW BeerView WITH SCHEMABINDING AS SELECT ID, Description FROM dbo.Beer",
				@"CREATE UNIQUE CLUSTERED INDEX IX_BeerView ON BeerView (ID)",
			},
		};

		#region Install and Drop Tests
		/// <summary>
		/// Run through each of the schemas and make sure that they install properly.
		/// </summary>
		/// <param name="connectionString">The connection string to test against.</param>
		/// <param name="schema">The schema to install.</param>
		[Test]
		public void TestInstallSchemas(
			[ValueSource("ConnectionStrings")] string connectionString,
			[ValueSource("Schemas")] IEnumerable<string> schema)
		{
			TestWithRollback(connectionString, connection =>
			{
				// try to install the schema
				Install(connection, schema);

				// verify that they are there
				VerifyObjectsAndRegistry(schema, connection);
			});
		}

		/// <summary>
		/// Install schemas and then try to drop one object at a time.
		/// </summary>
		/// <param name="connectionString">The connection string to test against.</param>
		/// <param name="schema">The schema to install.</param>
		[Test]
		public void TestDropObjects(
			[ValueSource("ConnectionStrings")] string connectionString,
			[ValueSource("Schemas")] IEnumerable<string> schema)
		{
			TestWithRollback(connectionString, connection =>
			{
				while (schema.Any())
				{
					// try to install the schema and verify that they are there
					Install(connection, schema);
					VerifyObjectsAndRegistry(schema, connection);

					// remove the last object from the schema and try again
					schema = schema.Take(schema.Count() - 1);
				}
			});
		}
		#endregion

		#region Uninstall Tests
		/// <summary>
		/// Make sure that we can uninstall any of the test schemas we are working with.
		/// </summary>
		/// <param name="connectionString">The connection string to test against.</param>
		/// <param name="schema">The schema to uninstall.</param>
		[Test]
		public void TestUninstall(
			[ValueSource("ConnectionStrings")] string connectionString,
			[ValueSource("Schemas")] IEnumerable<string> schema)
		{
			TestWithRollback(connectionString, connection =>
			{
				// try to install the schema and verify that they are there
				Install(connection, schema);
				VerifyObjectsAndRegistry(schema, connection);

				// uninstall it
				SchemaInstaller installer = new SchemaInstaller(connection);
				installer.Uninstall(TestSchemaGroup);

				// make sure the registry is empty
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);
				Assert.IsTrue(!registry.Entries.Any());

				// make sure all of the objects exist in the database
				foreach (var schemaObject in schema.Select(s => new SchemaObject(s)))
					Assert.False(schemaObject.Exists(connection), "Object {0} is not deleted from database", schemaObject.Name);
			});
		}
		#endregion

		#region Modify Tests
		/// <summary>
		/// Make sure that we can modify any of the test schemas we are working with.
		/// </summary>
		/// <param name="connectionString">The connection string to test against.</param>
		/// <param name="schema">The schema to uninstall.</param>
		[Test]
		public void TestModify(
			[ValueSource("ConnectionStrings")] string connectionString,
			[ValueSource("Schemas")] IEnumerable<string> schema)
		{
			TestWithRollback(connectionString, connection =>
			{
				// try to install the schema and verify that they are there
				Install(connection, schema);
				VerifyObjectsAndRegistry(schema, connection);

				// now modify each schema object one at a time
				List<string> modifiedSchema = schema.ToList();
				for (int i = modifiedSchema.Count - 1; i >= 0; i--)
				{
					// modify the schema
					modifiedSchema[i] = modifiedSchema[i] + " -- MODIFIED ";

					// install the modified schema
					Install(connection, modifiedSchema);
					VerifyObjectsAndRegistry(modifiedSchema, connection);
				}
			});
		}
		#endregion

		#region Table Modify With Dependencies Test
		/// <summary>
		/// This is the set of schemas that we want to convert between. The test case tries all permutations of migrating from one to the other.
		/// </summary>
		private static List<string> _tableModifySchemas = new List<string> ()
		{
			// add columns of various types
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](128))",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256))",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [OriginalGravity][decimal](18,4) NOT NULL)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [OriginalGravity][decimal](18,4) NOT NULL, [Stuff][xml] NULL)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [OriginalGravity][decimal](18,4) NOT NULL, [Stuff][xml] NULL, [ChangeDate][rowversion])",

			// drop columns and add them at the same time
			// test identity creation
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [NewID] [int] IDENTITY (10, 10))",

			// modify some types
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [ModifyDecimal] [decimal](18, 0) NOT NULL)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [ModifyDecimal] [decimal](18, 0) NULL)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [ModifyDecimal] [decimal](18, 2) NULL)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [ModifyString] [varchar](32))",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [ModifyString] [varchar](MAX))",

			// add a computed column
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [OneMore] AS ID+1)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](256), [OneMore] AS ID+2)",

			// add a column with an inline default
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](128), [Foo] [int] NULL)",
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [Description][varchar](128), [Foo] [int] NULL DEFAULT (0))",

			// change the case of columns
			"CREATE TABLE Beer ([ID] [int] NOT NULL, [description][varchar](128))",
		};

		private static List<string> _tableAdditionalSchema = new List<string>()
		{
			@"ALTER TABLE [Beer] ADD CONSTRAINT PK_Beer PRIMARY KEY ([ID])",
			@"ALTER TABLE [Beer] ADD CONSTRAINT CK_BeerTable CHECK (ID > 0)",
			@"ALTER TABLE [Beer] ADD CONSTRAINT CK_BeerColumn CHECK (ID > 5)",
			@"ALTER TABLE [Beer] ADD CONSTRAINT DF_Beer_Description DEFAULT 'IPA' FOR Description",
			@"CREATE VIEW BeerView AS SELECT * FROM Beer",
			@"CREATE VIEW BeerView2 WITH SCHEMABINDING AS SELECT ID, Description FROM dbo.Beer",
			@"CREATE NONCLUSTERED INDEX IX_Beer ON Beer (Description)",
			@"CREATE PROC [BeerProc] AS SELECT * FROM BeerView",
			@"GRANT EXEC ON [BeerProc] TO [public]",
			@"CREATE FUNCTION [BeerFunc] () RETURNS [int] AS BEGIN DECLARE @i [int] SELECT @i=MAX(ID) FROM BeerView RETURN @i END",
			@"CREATE FUNCTION [BeerTableFunc] () RETURNS @IDs TABLE (ID [int]) AS BEGIN INSERT INTO @IDs SELECT ID FROM BeerView RETURN END",
			@"CREATE TABLE Keg ([ID] [int], [BeerID] [int])",
			@"ALTER TABLE [Keg] ADD CONSTRAINT FK_Keg_Beer FOREIGN KEY ([BeerID]) REFERENCES Beer (ID)",
			@"-- AUTOPROC All [Beer]",
		};

		[Test]
		public void TestTableModify(
			[ValueSource("ConnectionStrings")] string connectionString,
			[ValueSource("_tableModifySchemas")] string initialTable,
			[ValueSource("_tableModifySchemas")] string finalTable)
		{
			TestWithRollback(connectionString, connection =>
			{
				// create the initial table with additional dependencies
				List<string> schema = new List<string>() { initialTable };
				schema.AddRange(_tableAdditionalSchema);
				InstallAndVerify(connection, schema);

				// create the new final table and install that
				schema = new List<string>() { finalTable };
				schema.AddRange(_tableAdditionalSchema);
				InstallAndVerify(connection, schema);
			});
		}
		#endregion

		#region Column Default Tests
		/// <summary>
		/// This is the set of schemas that we want to convert between. The test case tries all permutations of migrating from one to the other.
		/// </summary>
		private static List<IEnumerable<string>> _defaultSchemas = new List<IEnumerable<string>> ()
		{
			// no defaults
			new string[] { @"CREATE TABLE Beer ([ID] [int], [Description][varchar](256))" },
			// table modify
			new string[] { @"CREATE TABLE Beer ([ID] [int], [Description][varchar](256), [Foo][varchar](128))", @"ALTER TABLE Beer ADD DEFAULT (0) FOR [ID]",},

			// TWO inline named defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int] CONSTRAINT DF_Beer_ID DEFAULT (0), [Description][varchar](256) CONSTRAINT DF_Beer_Default DEFAULT ('Foo'))" },
			// TWO inline anonymous defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int] DEFAULT (0), [Description][varchar](256) DEFAULT ('Foo'))" },
			// TWO explicit named defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int], [Description][varchar](256))", @"ALTER TABLE Beer ADD CONSTRAINT DF_Beer_ID DEFAULT (0) FOR [ID]", @"ALTER TABLE Beer ADD CONSTRAINT DF_Beer_Description DEFAULT ('Foo') FOR [Description]" },
			// TWO explicit anonymous defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int], [Description][varchar](256))", @"ALTER TABLE Beer ADD DEFAULT (0) FOR [ID]", @"ALTER TABLE Beer ADD DEFAULT ('Foo') FOR [Description]" },

			// CHANGED TWO inline named defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int] CONSTRAINT DF_Beer_ID DEFAULT (1), [Description][varchar](256) CONSTRAINT DF_Beer_Default DEFAULT 'Moo')" },
			// CHANGED TWO inline anonymous defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int] DEFAULT (1), [Description][varchar](256) DEFAULT 'Moo')" },
			// CHANGED TWO explicit named defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int], [Description][varchar](256))", @"ALTER TABLE Beer ADD CONSTRAINT DF_Beer_ID DEFAULT (1) FOR [ID]", @"ALTER TABLE Beer ADD CONSTRAINT DF_Beer_Description DEFAULT 'Moo' FOR [Description]" },
			// CHANGED TWO explicit anonymous defaults
			new string [] { @"CREATE TABLE Beer ([ID] [int], [Description][varchar](256))", @"ALTER TABLE Beer ADD DEFAULT (1) FOR [ID]", @"ALTER TABLE Beer ADD DEFAULT 'Moo' FOR [Description]" },
		};

		/// <summary>
		/// Test migrating from all different forms of defaults.
		/// </summary>
		/// <param name="connectionString">The connection to test.</param>
		/// <param name="initialSchema">The schema to start from.</param>
		/// <param name="finalSchema">The schema to end with</param>
		[Test]
		public void TestModifyingDefaults([ValueSource("ConnectionStrings")] string connectionString,
			[ValueSource("_defaultSchemas")] IEnumerable<string> initialSchema,
			[ValueSource("_defaultSchemas")] IEnumerable<string> finalSchema
			)
		{
			TestWithRollback(connectionString, connection =>
			{
				// set up the initial schema
				InstallAndVerify(connection, initialSchema);
				Assert.AreEqual(initialSchema.Any(s => s.Contains("DEFAULT (0)")), DefaultExists(connection, "Beer", "ID", "((0))"));
				Assert.AreEqual(initialSchema.Any(s => s.Contains("DEFAULT (1)")), DefaultExists(connection, "Beer", "ID", "((1))"));
				Assert.AreEqual(initialSchema.Any(s => s.Contains("DEFAULT ('Foo')")), DefaultExists(connection, "Beer", "Description", "('Foo')"));
				Assert.AreEqual(initialSchema.Any(s => s.Contains("DEFAULT 'Moo'")), DefaultExists(connection, "Beer", "Description", "('Moo')"));

				// set up the final schema
				InstallAndVerify(connection, finalSchema);
				Assert.AreEqual(finalSchema.Any(s => s.Contains("DEFAULT (0)")), DefaultExists(connection, "Beer", "ID", "((0))"));
				Assert.AreEqual(finalSchema.Any(s => s.Contains("DEFAULT (1)")), DefaultExists(connection, "Beer", "ID", "((1))"));
				Assert.AreEqual(finalSchema.Any(s => s.Contains("DEFAULT ('Foo')")), DefaultExists(connection, "Beer", "Description", "('Foo')"));
				Assert.AreEqual(finalSchema.Any(s => s.Contains("DEFAULT 'Moo'")), DefaultExists(connection, "Beer", "Description", "('Moo')"));
			});
		}


		/// <summary>
		/// There are more restrictions on modifying defaults on tables that have data in them.
		/// </summary>
		/// <param name="connectionString">The connection to test.</param>
		[Test]
		public void TestModifyingDefaultsWithData([ValueSource("ConnectionStrings")] string connectionString)
		{
			TestWithRollback(connectionString, connection =>
			{
				// set up the initial schema
				InstallAndVerify(connection, new[] { "CREATE TABLE Beer ([ID] [int] NULL)" });
				connection.ExecuteSql(@"INSERT INTO Beer VALUES (NULL)");

				// try to convert the column to have a default
				InstallAndVerify(connection, new[] { "CREATE TABLE Beer ([ID] [int] NULL DEFAULT (0))" });

				// try to add a non nullable column with a default
				InstallAndVerify(connection, new[] { "CREATE TABLE Beer ([ID] [int] NULL DEFAULT (0), Style [varchar](128) NOT NULL DEFAULT ('IPA'))" });
			});
		}
		#endregion

		#region Helper Functions
		/// <summary>
		/// Verify all of the objects in the database and registry.
		/// </summary>
		/// <param name="schema">The schema to verify.</param>
		/// <param name="connection">The connection to use.</param>
		private static void VerifyObjectsAndRegistry(IEnumerable<string> schema, RecordingDbConnection connection)
		{
			connection.DoNotLog(() =>
			{
				// make sure the schema registry was updated
				SchemaRegistry registry = new SchemaRegistry(connection, TestSchemaGroup);

				// make sure all of the objects exist in the database
				foreach (var schemaObject in schema.Select(s => new SchemaObject(s)))
				{
					// azure doesn't support xml index, so lets comment those out
					if (schemaObject.Sql.Contains("XML INDEX") && connection.ConnectionString.Contains("windows.net"))
						continue;

					Assert.True(schemaObject.Exists(connection), "Object {0} is missing from database", schemaObject.Name);
					Assert.True(registry.Contains(schemaObject), "Object {0} is missing from registry", schemaObject.Name);
				}
			});
		}

		/// <summary>
		/// Install a schema into a database.
		/// </summary>
		/// <param name="connection">The connection to use.</param>
		/// <param name="sql"the SQL to install.</param>
		private static void Install(DbConnection connection, IEnumerable<string> sql)
		{
			SchemaInstaller installer = new SchemaInstaller(connection);
			SchemaObjectCollection schema = new SchemaObjectCollection();
			if (sql != null)
			{
				foreach (string s in sql)
				{
					// azure doesn't support xml index, so lets comment those out
					if (s.Contains("XML INDEX") && connection.ConnectionString.Contains("windows.net"))
						continue;

					schema.Add(s);
				}
			}

			installer.Install("test", schema);
		}

		/// <summary>
		/// Install a schema into a database.
		/// </summary>
		/// <param name="connection">The connection to use.</param>
		/// <param name="sql"the SQL to install.</param>
		private static void InstallAndVerify(RecordingDbConnection connection, IEnumerable<string> sql)
		{
			Install(connection, sql);
			VerifyObjectsAndRegistry(sql, connection);
		}

		/// <summary>
		/// Verify that a default exists on a column.
		/// </summary>
		/// <param name="connection">The connection to test.</param>
		/// <param name="table">The name of the table.</param>
		/// <param name="column">The name of the column.</param>
		/// <param name="value">If specified, the expected value of the default.</param>
		/// <returns>True if the default exists as expected.</returns>
		private bool DefaultExists(RecordingDbConnection connection, string table, string column, string value = null)
		{
			return connection.DoNotLog(() =>
			{
				string definition = connection.ExecuteScalarSql<string>(@"SELECT definition
					FROM sys.default_constraints d
					JOIN sys.objects o ON (d.parent_object_id = o.object_id)
					JOIN sys.columns c ON (d.parent_object_id = c.object_id AND d.parent_column_id = c.column_id)
					WHERE o.name = @TableName AND c.name = @ColumnName",
					new { TableName = table, ColumnName = column });

				if (definition == null)
					return false;

				if (value == null)
					return true;

				return definition == value;
			});
		}
		#endregion
	}
}
