#region Using directives
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Transactions;
using Insight.Database.Schema.Properties;
using System.Data.Common;
#endregion

namespace Insight.Database.Schema
{
    #region SchemaInstaller Class
    /// <summary>
    /// Installs, upgrades, and uninstalls objects from a database
    /// </summary>
    public sealed class SchemaInstaller
    {
		#region Constructors
		/// <summary>
		/// Initialize the SchemaInstaller to work with a given SqlConnection.
		/// </summary>
		/// <param name="connection">The SqlConnection to work with.</param>
		public SchemaInstaller(DbConnection connection)
		{
			if (connection == null)
				throw new ArgumentNullException("connection");

			// require the connection to be open
			if (connection.State != ConnectionState.Open)
				throw new ArgumentException("connection must be in an Open state.", "connection");

			// save the connection - make sure we are recording one way or another
			_connection = connection as RecordingDbConnection ?? new RecordingDbConnection (connection);
		}
		#endregion

        #region Database Utility Methods
		/// <summary>
		/// Check to see if the database exists
		/// </summary>
		/// <param name="connectionString">The connection string for the database to connect to.</param>
		/// <returns>True if the database already exists</returns>
		public static bool DatabaseExists(string connectionString)
		{
			if (connectionString == null)
				throw new ArgumentNullException("connectionString");

			SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
			string databaseName = builder.InitialCatalog;

			using (var connection = OpenMasterConnection(connectionString))
			{
				var command = new SqlCommand("SELECT COUNT (*) FROM master.sys.databases WHERE name = @DatabaseName", connection);
				command.Parameters.AddWithValue("@DatabaseName", databaseName);

				return ((int)command.ExecuteScalar()) > 0;
			}
		}

        /// <summary>
        /// Create a database on the specified connection if it does not exist.
        /// </summary>
        /// <returns>True if the database was created, false if it already exists.</returns>
        /// <exception cref="SqlException">If the database name is invalid.</exception>
		public static bool CreateDatabase (string connectionString)
        {
			if (connectionString == null)
				throw new ArgumentNullException("connectionString");

			// see if the database already exists
			if (DatabaseExists(connectionString))
				return false;

			SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
			string databaseName = builder.InitialCatalog;

			using (var connection = OpenMasterConnection(connectionString))
			{
				var command = new SqlCommand(String.Format(CultureInfo.InvariantCulture, "CREATE DATABASE [{0}]", databaseName), connection);
				command.ExecuteNonQuery();
            }

			return true;
		}

        /// <summary>
        /// Drop a database if it exists.
        /// </summary>
        /// <returns>True if the database was dropped, false if it did not exist.</returns>
        /// <exception cref="SqlException">If the database name is invalid or cannot be dropped.</exception>
		public static bool DropDatabase(string connectionString)
        {
			if (connectionString == null)
				throw new ArgumentNullException("connectionString");

			// see if the database was already dropped
			if (!DatabaseExists(connectionString))
				return false;

			SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
			string databaseName = builder.InitialCatalog;

			using (var connection = OpenMasterConnection(connectionString))
			{
				// attempt to set the database to single user mode
                // set the database to single user mode, effectively dropping all connections except the current
                // connection.
				try
				{
					var command = new SqlCommand(String.Format(CultureInfo.InvariantCulture, "ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", databaseName), connection);
					command.ExecuteNonQuery();
				}
				catch (SqlException)
				{
					// eat any exception here
					// Azure will complain that this is a syntax error
					// SQL - The database may already be in single user mode
				}

				// attempt to drop the database
				var dropCommand = new SqlCommand(String.Format(CultureInfo.InvariantCulture, "DROP DATABASE [{0}]", databaseName), connection);
				dropCommand.ExecuteNonQuery();
            }

			return true;
		}

		/// <summary>
		/// Gets the connection string needed to connect to the master database.
		/// This is used when creating/dropping databases.
		/// </summary>
		/// <param name="connectionString">The target connectionString.</param>
		/// <returns>The connection string pointing at the master database.</returns>
		private static string GetMasterConnectionString(string connectionString)
		{
			SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
			builder.InitialCatalog = "master";
			return builder.ConnectionString;
		}

		/// <summary>
		/// Opens a connection needed to connect to the master database.
		/// This is used when creating/dropping databases.
		/// </summary>
		/// <param name="connectionString">The target connectionString.</param>
		/// <returns>The connection string pointing at the master database.</returns>
		private static SqlConnection OpenMasterConnection(string connectionString)
		{
			var connection = new SqlConnection(GetMasterConnectionString(connectionString));
			connection.Open();
			return connection;
		}
    	#endregion  

		#region Schema Installation Methods
		/// <summary>
		/// Script the changes that would be applied to the database.
		/// </summary>
		/// <param name="schemaGroup">The name of the schemaGroup.</param>
		/// <param name="schema">The schema to install.</param>
		/// <returns>The script representing the changes to the database.</returns>
		public string ScriptChanges(string schemaGroup, SchemaObjectCollection schema)
		{
			// create a transaction around the installation
			// NOTE: don't commit the changes to the database
			// WARNING: due to the way we script autoprocs (and maybe others), this has to modify the database, then roll back the changes
			//	so you might not want to try this on a live production database. Go get a copy of your database, then do the scripting on a staging environment.
			using (TransactionScope transaction = new TransactionScope(TransactionScopeOption.Required, new TimeSpan(1, 0, 0, 0, 0)))
			{
				_connection.OnlyRecord(() => Install(schemaGroup, schema));
			}

			return _connection.ScriptLog.ToString();
		}

		/// <summary>
		/// Install a schema into a database.
		/// </summary>
		/// <param name="schemaGroup">The name of the schemaGroup.</param>
		/// <param name="schema">The schema to install.</param>
		public void Install(string schemaGroup, SchemaObjectCollection schema)
		{
			_connection.ResetLog();

			// validate the arguments
			if (schemaGroup == null) throw new ArgumentNullException("schemaGroup");
			if (schema == null) throw new ArgumentNullException("schema");

			// make sure the schema objects are valid
			schema.Validate();

			// get the list of objects to install, filtering out the extra crud
			InstallContext context = new InstallContext();
			context.SchemaObjects = OrderSchemaObjects(schema.Where(o => o.SchemaObjectType != SchemaObjectType.Unused));

			// azure doesn't support filegroups or partitions, so we need to know if we are on azure
			context.IsAzure = _connection.ExecuteScalarSql<bool>("SELECT CONVERT(bit, CASE WHEN SERVERPROPERTY('edition') = 'SQL Azure' THEN 1 ELSE 0 END)", Parameters.Empty);

			using (TransactionScope transaction = new TransactionScope(TransactionScopeOption.Required, new TimeSpan(1, 0, 0, 0, 0)))
			{
				// load the schema registry from the database
				context.SchemaRegistry = new SchemaRegistry(_connection, schemaGroup);

				// find all of the objects that we need to drop
				context.DropObjects = context.SchemaRegistry.Entries
					.Where(e => !context.SchemaObjects.Any(o => String.Compare(e.ObjectName, o.Name, StringComparison.OrdinalIgnoreCase) == 0))
					.ToList();

				// sort to drop in reverse dependency order 
				context.DropObjects.Sort((e1, e2) => -CompareByInstallOrder(e1, e2));

				// find all of the objects that are new
				context.AddObjects = context.SchemaObjects.Where(o => !context.SchemaRegistry.Contains(o)).ToList();
				context.AddObjects.Sort(CompareByInstallOrder);

				// find all of the objects that have changed
				_connection.DoNotLog(() =>
				{
					foreach (var change in context.SchemaObjects.Where(o => context.SchemaRegistry.Find(o) != null && context.SchemaRegistry.Find(o).Signature != o.GetSignature(_connection, schema)))
						ScriptUpdate(context, change);
				});

				// sort the objects in install order
				context.AddObjects.Sort(CompareByInstallOrder);

				// make the changes
				DropObjects(context.DropObjects);
				AddObjects(context.AddObjects, context.SchemaObjects);
				VerifyObjects(context.SchemaObjects);

				// update the schema registry
				context.SchemaRegistry.Update(context.SchemaObjects);

				// complete the changes
				transaction.Complete();
			}
		}

		/// <summary>
		/// Uninstall a schema group from the database.
		/// </summary>
		/// <remarks>This is a transactional operation</remarks>
		/// <param name="schemaGroup">The group to uninstall</param>
		/// <exception cref="ArgumentNullException">If schemaGroup is null</exception>
		/// <exception cref="SqlException">If any object fails to uninstall</exception>
		public void Uninstall(string schemaGroup)
		{
			// validate the arguments
			if (schemaGroup == null) throw new ArgumentNullException("schemaGroup");

			// create an empty collection and install that
			SchemaObjectCollection objects = new SchemaObjectCollection();
			Install(schemaGroup, objects);
		}

		/// <summary>
		/// Determine if the given schema has differences with the current schema.
		/// </summary>
		/// <param name="schemaGroup">The schema group to compare.</param>
		/// <param name="schema">The schema to compare with.</param>
		/// <returns>True if there are any differences.</returns>
		public bool Diff(string schemaGroup, SchemaObjectCollection schema)
		{
			// validate the arguments
			if (schemaGroup == null) throw new ArgumentNullException("schemaGroup");
			if (schema == null) throw new ArgumentNullException("schema");

			SchemaRegistry registry = new SchemaRegistry(_connection, schemaGroup);

			// if any objects are missing from the registry, then there is a difference
			if (schema.Any(o => registry.Find(o.Name) == null))
				return true;

			// if there are any registry entries missing from the new schema, there is a difference
			if (registry.Entries.Any(e => !schema.Any(o => String.Compare(e.ObjectName, o.Name, StringComparison.OrdinalIgnoreCase) == 0)))
				return true;

			// if there are any matches, but have different signatures, there is a difference
			if (schema.Any(o => registry.Find(o.Name).Signature != o.GetSignature(_connection, schema)))
				return true;

			// didn't detect differences
			return false;
		}
		#endregion

		#region Scripting Methods
        /// <summary>
        /// Schedule an update by adding the appropriate delete, update and add records
        /// </summary>
		/// <param name="context">The installation context.</param>
        /// <param name="schemaObject">The object to update.</param>
		private void ScriptUpdate(InstallContext context, SchemaObject schemaObject)
        {
			// if we have already scripted this object, then don't do it again
			if (context.AddObjects.Any(o => o.Name == schemaObject.Name))
				return;

			// if this is a table, then let's see if we can just modify the table
			if (schemaObject.SchemaObjectType == SchemaObjectType.Table)
			{
				ScriptStandardDependencies(context, schemaObject);
				ScriptTableUpdate(context, schemaObject);
				return;
			}

			// add the object to the add queue before anything that depends on it, as well as any permissions on the object
			if (context.AddObjects.Any())
				schemaObject.OriginalOrder = context.AddObjects.Max(o => o.OriginalOrder) + 1;
			else
				schemaObject.OriginalOrder = 1;
			context.AddObjects.Add(schemaObject);

			// don't log any of our scripting
			ScriptPermissions(context, schemaObject);
			ScriptStandardDependencies(context, schemaObject);

			// handle dependencies for different types of objects
			if (schemaObject.SchemaObjectType == SchemaObjectType.IndexedView)
			{
				ScriptIndexes(context, schemaObject);
			}
			else if (schemaObject.SchemaObjectType == SchemaObjectType.PrimaryKey)
			{
				ScriptForeignKeys(context, schemaObject);
				ScriptXmlIndexes(context, schemaObject);
			}
			else if (schemaObject.SchemaObjectType == SchemaObjectType.PrimaryXmlIndex)
			{
				ScriptXmlIndexes(context, schemaObject);
			}
			else if (schemaObject.SchemaObjectType == SchemaObjectType.Index)
			{
				ScriptIndexes(context, schemaObject);
			}

			// drop the object after any dependencies are dropped
			SchemaRegistryEntry dropEntry = context.SchemaRegistry.Find(schemaObject.Name);
			if (dropEntry == null)
			{
				dropEntry = new SchemaRegistryEntry()
				{
					Type = schemaObject.SchemaObjectType,
					ObjectName = schemaObject.Name
				};
			}

			context.DropObjects.Add(dropEntry);
        }

		private const string InsightTemp = "Insight__tmp_";

		#region Table Update Methods
		/// <summary>
		/// Script the update of a table.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The object to update.</param>
		private void ScriptTableUpdate(InstallContext context, SchemaObject schemaObject)
		{
			string oldTableName = schemaObject.Name;
			string newTableName = InsightTemp + DateTime.Now.Ticks.ToString(CultureInfo.InvariantCulture);

			try
			{
				// make a temporary table so we can analyze the difference
				// note that we rename the table and its constraints so that we don't have conflicts when creating it
				string tempTable = schemaObject.Sql;
				tempTable = createTableRegex.Replace(tempTable, "CREATE TABLE " + newTableName);
				tempTable = constraintRegex.Replace(tempTable, match => "CONSTRAINT " + SqlParser.FormatSqlName(InsightTemp + SqlParser.UnformatSqlName(match.Groups[1].Value)));
				_connection.ExecuteSql(tempTable);

				// detect if the table was created on a different data space and throw
				var oldDataSpace = _connection.ExecuteScalarSql<int>("SELECT data_space_id FROM sys.indexes i WHERE i.object_id = OBJECT_ID(@TableName) AND type <= 1", new { TableName = oldTableName });
				var newDataSpace = _connection.ExecuteScalarSql<int>("SELECT data_space_id FROM sys.indexes i WHERE i.object_id = OBJECT_ID(@TableName) AND type <= 1", new { TableName = newTableName });
				if (oldDataSpace != newDataSpace)
					throw new SchemaException(String.Format(CultureInfo.InvariantCulture, "Cannot move table {0} to another filegroup or partition", oldTableName));

				// script constraints before columns because constraints depend on columns
				ScriptConstraints(context, oldTableName, newTableName);
				ScriptColumns(context, schemaObject, oldTableName, newTableName);
			}
			finally
			{
				try
				{
					// clean up the temporary table
					_connection.ExecuteSql("DROP TABLE " + newTableName);
				}
				catch (SqlException)
				{
					// eat this and throw the original error
				}
			}
		}

		// detect the name of the table and replace it.
		private static Regex createTableRegex = new Regex(String.Format(CultureInfo.InvariantCulture, @"CREATE\s+TABLE\s+{0}", SqlParser.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.Compiled);
		private static Regex constraintRegex = new Regex(String.Format(CultureInfo.InvariantCulture, @"CONSTRAINT\s+({0})", SqlParser.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.Compiled);

		/// <summary>
		/// Script changes to a table column.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The table object that we are modifying.</param>
		/// <param name="oldTableName">The old name of the table.</param>
		/// <param name="newTableName">The new name of the table.</param>
		private void ScriptColumns(InstallContext context, SchemaObject schemaObject, string oldTableName, string newTableName)
		{
			Func<dynamic, dynamic, bool> compareColumns = (dynamic c1, dynamic c2) => (c1.Name == c2.Name);
			Func<dynamic, dynamic, bool> areColumnsEqual = (dynamic c1, dynamic c2) =>
				c1.TypeName == c2.TypeName &&
				c1.MaxLength == c2.MaxLength &&
				c1.Precision == c2.Precision &&
				c1.Scale == c2.Scale &&
				c1.IsNullable == c2.IsNullable &&
				c1.IsIdentity == c2.IsIdentity &&
				c1.IdentitySeed == c2.IdentitySeed &&
				c1.IdentityIncrement == c2.IdentityIncrement &&
				c1.Definition == c2.Definition;

			// get the columns for each of the tables
			var oldColumns = GetColumnsForTable(oldTableName);
			var newColumns = GetColumnsForTable(newTableName);

			var missingColumns = oldColumns.Except(newColumns, compareColumns).ToList();
			var addColumns = newColumns.Except(oldColumns, compareColumns).ToList();

			// calculate which ones changed and add them to both lists
			var changedColumns = newColumns.Where((dynamic cc) =>
			{
				dynamic oldColumn = oldColumns.FirstOrDefault(oc => compareColumns(cc, oc));

				return (oldColumn != null) && !areColumnsEqual(oldColumn, cc);
			}).ToList();

			// if we want to modify a computed column, we have to drop/add it
			var changedComputedColumns = changedColumns.Where(c => c.Definition != null).ToList();
			foreach (var cc in changedComputedColumns)
			{
				missingColumns.Add(cc);
				addColumns.Add(cc);
				changedColumns.Remove(cc);
			}

			// delete old columns - this should be pretty free
			if (missingColumns.Any())
			{
				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("ALTER TABLE {0}", SqlParser.FormatSqlName(oldTableName));
				sb.Append(" DROP");
				sb.AppendLine(String.Join(",", missingColumns.Select((dynamic o) => String.Format(" COLUMN {0}", SqlParser.FormatSqlName(o.Name)))));
				context.AddObjects.Add(new SchemaObject(SchemaObjectType.Table, oldTableName, sb.ToString()));
			}

			// add new columns - this is free when the columns are nullable
			if (addColumns.Any())
			{
				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("ALTER TABLE {0}", SqlParser.FormatSqlName(oldTableName));
				sb.Append(" ADD ");
				sb.AppendLine(String.Join(", ", addColumns.Select((dynamic o) => GetColumnDefinition(o))));
				context.AddObjects.Add(new SchemaObject(SchemaObjectType.Table, oldTableName, sb.ToString()));
			}

			// alter columns
			foreach (dynamic column in changedColumns)
			{
				// find any indexes that are on that column
				ScriptIndexes(context, schemaObject, column.Name);

				// script the alter
				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("ALTER TABLE {0} ALTER COLUMN ", SqlParser.FormatSqlName(oldTableName));
				sb.AppendFormat(GetColumnDefinition(column));
				context.AddObjects.Add(new SchemaObject(SchemaObjectType.Table, oldTableName, sb.ToString()));
			}
		}

		/// <summary>
		/// Script constraints on a table. This currently only handles defaults.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="oldTableName">The name of the old table.</param>
		/// <param name="newTableName">The name of the new table.</param>
		private void ScriptConstraints(InstallContext context, string oldTableName, string newTableName)
		{
			// compare constraints by column then by name (if they are named)
			Func<dynamic, dynamic, bool> compareConstraints = (dynamic c1, dynamic c2) => (c1.ColumnID == c2.ColumnID) && ((c1.Name == c2.Name) || (c1.IsSystemNamed && c2.IsSystemNamed));
			Func<dynamic, string> getConstraintName = (dynamic c) => SqlParser.FormatSqlName(oldTableName) + "." + SqlParser.FormatSqlName(c.ColumnName);

			// go through all of the system-named constraints on the table
			// filter out any old constraints that we are dropping because we dropped the explicit constraint
			var oldConstraints = GetConstraintsForTable(oldTableName)
				.Where(c => !context.SchemaRegistry.Contains(getConstraintName(c)))
				.ToList();
			var newConstraints = GetConstraintsForTable(newTableName);

			// detect missing and new constraints
			var missingConstraints = oldConstraints.Except(newConstraints, compareConstraints).ToList();
			var addConstraints = newConstraints.Except(oldConstraints, compareConstraints).ToList();

			// calculate which ones changed and add them to both lists
			var changedConstraints = newConstraints.Where((dynamic cc) =>
				{
					dynamic oldConstraint = oldConstraints.FirstOrDefault(oc => compareConstraints(cc, oc));

					return (oldConstraint != null) && (oldConstraint.Definition != cc.Definition);
				});
			missingConstraints.AddRange(changedConstraints.Select(c => oldConstraints.First(o => compareConstraints(c, o))));
			addConstraints.AddRange(changedConstraints);

			// delete old constraints
			if (missingConstraints.Any())
			{
				foreach (var missingConstraint in missingConstraints)
				{
					var dropObject = new SchemaRegistryEntry() { Type = SchemaObjectType.Default, ObjectName = SqlParser.FormatSqlName(oldTableName) + "." + SqlParser.FormatSqlName(missingConstraint.ColumnName) }; 
					if (!context.DropObjects.Any(o => o.ObjectName == dropObject.ObjectName))
						context.DropObjects.Add(dropObject);
				}
			}

			// add new constraints
			if (addConstraints.Any())
			{
				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("ALTER TABLE {0}", SqlParser.FormatSqlName(oldTableName));
				sb.Append(" ADD");
				sb.AppendLine(String.Join(",", addConstraints.Select((dynamic o) => GetDefaultDefinition(o))));
				context.AddObjects.Add(new SchemaObject(SchemaObjectType.Default, oldTableName, sb.ToString()));
			}
		}

		/// <summary>
		/// Get the columns for a table.
		/// </summary>
		/// <param name="tableName">The name of the table.</param>
		/// <returns>The list of columns on the table.</returns>
		private IEnumerable<FastExpando> GetColumnsForTable(string tableName)
		{
			return _connection.QuerySql(@"SELECT Name=c.name, ColumnID=c.column_id, TypeName = t.name, MaxLength=c.max_length, Precision=c.precision, scale=c.scale, IsNullable=c.is_nullable, IsIdentity=c.is_identity, IdentitySeed=i.seed_value, IdentityIncrement=i.increment_value, Definition=cc.definition
				FROM sys.columns c
				JOIN sys.types t ON (c.system_type_id = t.system_type_id AND c.user_type_id = t.user_type_id)
				LEFT JOIN sys.identity_columns i ON (c.object_id = i.object_id AND c.column_id = i.column_id)
				LEFT JOIN sys.computed_columns cc ON (cc.object_id = c.object_id AND cc.column_id = c.column_id)
				WHERE c.object_id = OBJECT_ID (@TableName)",
				new { TableName = tableName });
		}

		/// <summary>
		/// Get the constraints for a table.
		/// </summary>
		/// <param name="tableName">The name of the table.</param>
		/// <returns>The list of constraints  on the table.</returns>
		private IEnumerable<FastExpando> GetConstraintsForTable(string tableName)
		{
			return _connection.QuerySql(String.Format(CultureInfo.InvariantCulture, @"
				SELECT name=REPLACE(d.Name, '{0}', ''), ColumnID=d.parent_column_id, ColumnName=c.name, TypeName=d.type_desc, IsSystemNamed=d.is_system_named, Definition=d.definition 
					FROM sys.default_constraints d 
					JOIN sys.columns c ON (d.parent_object_id = c.object_id AND d.parent_column_id = c.column_id)
					WHERE d.parent_object_id = OBJECT_ID(@TableName)
/*
				UNION 
				SELECT name=c.Name, ColumnID=c.parent_column_id, TypeName=c.type_desc, definition=c.definition FROM sys.check_constraints c WHERE parent_object_id = OBJECT_ID(@TableName)
				UNION 
				SELECT name=c.Name, ColumnID=NULL, TypeName=c.type_desc, definition=NULL FROM sys.key_constraints c WHERE parent_object_id = OBJECT_ID(@TableName)
				UNION 
				SELECT name=c.Name, ColumnID=NULL, TypeName=c.type_desc, definition=NULL FROM sys.foreign_keys c WHERE parent_object_id = OBJECT_ID(@TableName)
*/
				", InsightTemp),
				new { TableName = tableName }).ToList();

		}

		/// <summary>
		/// Output the definition of a column.
		/// </summary>
		/// <param name="column">The column object.</param>
		/// <returns>The string definition of the column.</returns>
		private static string GetColumnDefinition(dynamic column)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(SqlParser.FormatSqlName(column.Name));

			if (column.Definition == null)
			{
				// this is a regular column, add in the type of the column
				sb.AppendFormat(" {0}", column.TypeName);

				string typeName = column.TypeName;
				switch (typeName)
				{
					case "nvarchar":
					case "varchar":
					case "varbinary":
						if (column.MaxLength == -1)
							sb.Append("(MAX)");
						else
							sb.AppendFormat("({0})", column.MaxLength);
						break;

					case "decimal":
					case "numeric":
						sb.AppendFormat("({0}, {1})", column.Precision, column.Scale);
						break;
				}

				if (column.IsIdentity)
					sb.AppendFormat(" IDENTITY ({0}, {1})", column.IdentitySeed, column.IdentityIncrement);

				if (!column.IsNullable)
					sb.Append(" NOT NULL");
			}
			else
			{
				// add in computed columns
				sb.AppendFormat(" AS {0}", column.Definition);
			}

			return sb.ToString();
		}

		/// <summary>
		/// Output the definition of a default.
		/// </summary>
		/// <param name="def">The default object.</param>
		/// <returns>The string definition of the column.</returns>
		private static string GetDefaultDefinition(dynamic def)
		{
			StringBuilder sb = new StringBuilder();

			// if there is an actual name, then name it
			if (!def.IsSystemNamed)
				sb.AppendFormat(" CONSTRAINT {0}", def.Name);

			// add the definition
			sb.Append(" DEFAULT ");
			sb.Append(def.Definition);
			sb.Append(" FOR ");
			sb.Append(SqlParser.FormatSqlName(def.ColumnName));

			return sb.ToString();
		}
		#endregion

		/// <summary>
		/// Script the permissions on an object and save the script to add the permissions back later
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The object to drop</param>
		private void ScriptPermissions(InstallContext context, SchemaObject schemaObject)
		{
			IList<FastExpando> permissions = null;

			if (schemaObject.SchemaObjectType == SchemaObjectType.Role)
			{
				// get the current permissions on the object
				permissions = _connection.QuerySql(@"SELECT UserName=u.name, Permission=p.permission_name, ClassType=p.class_desc, ObjectName=ISNULL(o.name, t.name)
								FROM sys.database_principals u
								JOIN sys.database_permissions p ON (u.principal_id = p.grantee_principal_id)
								LEFT JOIN sys.objects o ON (p.class_desc = 'OBJECT_OR_COLUMN' AND p.major_id = o.object_id)
								LEFT JOIN sys.types t ON (p.class_desc = 'TYPE' AND p.major_id = t.user_type_id)
								WHERE u.name = @ObjectName",
						new { ObjectName = SqlParser.UnformatSqlName(SqlParser.IndexNameFromFullName(schemaObject.Name.Split(' ')[1])) });
			}
			else
			{
				// get the current permissions on the object
				permissions = _connection.QuerySql(@"SELECT UserName=u.name, Permission=p.permission_name, ClassType=p.class_desc, ObjectName=ISNULL(o.name, t.name)
								FROM sys.database_principals u
								JOIN sys.database_permissions p ON (u.principal_id = p.grantee_principal_id)
								LEFT JOIN sys.objects o ON (p.class_desc = 'OBJECT_OR_COLUMN' AND p.major_id = o.object_id)
								LEFT JOIN sys.types t ON (p.class_desc = 'TYPE' AND p.major_id = t.user_type_id)
								WHERE ISNULL (o.name, t.name) = @ObjectName",
						new { ObjectName = SqlParser.UnformatSqlName(schemaObject.Name) });
			}

			// create a new permission schema object to install for each existing permission
			foreach (dynamic permission in permissions)
				context.AddObjects.Add(new SchemaObject(String.Format("GRANT {0} ON {1}{2} TO {3} -- DEPENDENCY", permission.Permission, permission.ClassType == "TYPE" ? "TYPE::" : "", SqlParser.FormatSqlName(permission.ObjectName), SqlParser.FormatSqlName(permission.UserName))));
		}

		/// <summary>
		/// Script the standard dependencies such as stored procs and triggers.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The schemaObject to script.</param>
		private void ScriptStandardDependencies(InstallContext context, SchemaObject schemaObject)
		{
			// find all of the dependencies on the object
			// this will find things that use views or tables
			// note that there will be more than one dependency if more than one column is referenced
			// ignore USER_TABLE, since that is calculated columns
			// for CHECK_CONSTRAINTS, ignore system-named constraints, since they are part of the table and will be handled there
			var dependencies = _connection.QuerySql(@"SELECT DISTINCT Name = o.name, SqlType = o.type_desc, IsSchemaBound=d.is_schema_bound_reference
				FROM sys.sql_expression_dependencies d
				JOIN sys.objects o ON (d.referencing_id = o.object_id)
				LEFT JOIN sys.check_constraints c ON (o.object_id = c.object_id)
				WHERE ISNULL(c.is_system_named, 0) = 0 AND
					o.type_desc <> 'USER_TABLE' AND 
					(o.parent_object_id = OBJECT_ID(@ObjectName) OR
					d.referenced_id =
						CASE WHEN d.referenced_class_desc = 'TYPE' THEN (SELECT user_type_id FROM sys.types t WHERE t.name = @ObjectName)
						ELSE OBJECT_ID(@ObjectName)
					END)",
				new { ObjectName = SqlParser.UnformatSqlName(schemaObject.Name) });

			foreach (dynamic dependency in dependencies)
			{
				// we only have to update schemabound dependencies
				if (schemaObject.SchemaObjectType == SchemaObjectType.Table && !dependency.IsSchemaBound)
					continue;

				// since the object isn't already being dropped, create a new SchemaObject for it and rebuild that
				SchemaObject dropObject = null;
				string dependencyType = dependency.SqlType;
				string dependencyName = dependency.Name;

				switch (dependencyType)
				{
					case "SQL_STORED_PROCEDURE":
					case "SQL_SCALAR_FUNCTION":
					case "SQL_TABLE_VALUED_FUNCTION":
					case "SQL_TRIGGER":
					case "VIEW":
						// these objects can be rebuilt from the definition of the object in the database
						dropObject = new SchemaObject(_connection.ExecuteScalarSql<string>("SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID(@Name)", new { Name = dependencyName }));
						break;

					case "CHECK_CONSTRAINT":
						// need to do a little work to re-create the check constraint
						dynamic checkConstraint = _connection.QuerySql(@"SELECT TableName=o.name, ConstraintName=c.name, Definition=c.definition
							FROM sys.check_constraints c
							JOIN sys.objects o ON (c.parent_object_id = o.object_id) WHERE c.object_id = OBJECT_ID(@Name)", new { Name = dependencyName }).First();

						dropObject = new SchemaObject(String.Format(
							"ALTER TABLE {0} ADD CONSTRAINT {1} CHECK {2}",
							SqlParser.FormatSqlName(checkConstraint.TableName),
							SqlParser.FormatSqlName(checkConstraint.ConstraintName),
							checkConstraint.Definition));
						break;

					default:
						throw new InvalidOperationException(String.Format(CultureInfo.InvariantCulture, "Cannot generate dependencies for object {0}.", dependencyName));
				}

				ScriptUpdate(context, dropObject);
			}
		}

		/// <summary>
		/// Script the foreign keys on a table or primary key.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The schemaObject to script.</param>
		private void ScriptForeignKeys(InstallContext context, SchemaObject schemaObject)
		{
			IList<FastExpando> foreignKeys = null;

			if (schemaObject.SchemaObjectType == SchemaObjectType.PrimaryKey)
			{
				foreignKeys = _connection.QuerySql(@"SELECT Name=f.name, TableName=o.name, RefTableName=ro.name, DeleteAction=delete_referential_action_desc, UpdateAction=update_referential_action_desc
					FROM sys.foreign_keys f
					JOIN sys.key_constraints k ON (f.referenced_object_id = k.parent_object_id)
					JOIN sys.objects o ON (f.parent_object_id = o.object_id)
					JOIN sys.objects ro ON (k.parent_object_id = ro.object_id)
					WHERE k.name = @ObjectName",
				new { ObjectName = SqlParser.IndexNameFromFullName(schemaObject.Name) });
			}
//			else
//			{
			//				foreignKeys = _connection.QuerySql(@"SELECT Name=f.name, TableName=o.name, RefTableName=r.name, DeleteAction=delete_referential_action_desc, UpdateAction=update_referential_action_desc
//					FROM sys.foreign_keys f
//					JOIN sys.objects o ON (f.parent_object_id = o.object_id)
//					JOIN sys.objects r ON (f.referenced_object_id = r.object_id)
//					WHERE f.referenced_object_id = OBJECT_ID(@ObjectName) OR f.parent_object_id = OBJECT_ID(@ObjectName)",
//				new { ObjectName = SqlParser.IndexNameFromFullName(schemaObject.Name) });
//			}

			foreach (dynamic foreignKey in foreignKeys)
			{
				// get the columns in the key from the database
				var columns = _connection.QuerySql(@"SELECT FkColumnName=fc.name, PkColumnName=kc.name
						FROM sys.foreign_key_columns f
						JOIN sys.columns fc ON (f.parent_object_id = fc.object_id AND f.parent_column_id = fc.column_id)
						JOIN sys.columns kc ON (f.referenced_object_id = kc.object_id AND f.referenced_column_id = kc.column_id)
						WHERE f.constraint_object_id = OBJECT_ID(@KeyName)",
					new { KeyName = foreignKey.Name });

				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY (",
						SqlParser.FormatSqlName(foreignKey.TableName),
						SqlParser.FormatSqlName(foreignKey.Name));
				sb.Append(String.Join(",", columns.Select((dynamic c) => SqlParser.FormatSqlName(c.FkColumnName))));
				sb.AppendFormat(") REFERENCES {0} (", SqlParser.FormatSqlName(foreignKey.RefTableName));
				sb.Append(String.Join(",", columns.Select((dynamic c) => SqlParser.FormatSqlName(c.PkColumnName))));
				sb.AppendFormat(") ON DELETE {0} ON UPDATE {1}", foreignKey.DeleteAction.Replace("_", " "), foreignKey.UpdateAction.Replace("_", " "));

				var dropObject = new SchemaObject(sb.ToString());

				ScriptUpdate(context, dropObject);
			}
		}

		/// <summary>
		/// Script the indexes on a table, view, or clustered index.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The schemaObject to script.</param>
		/// <param name="columnName">If specified, then this is the name of the column to filter on.</param>
		private void ScriptIndexes(InstallContext context, SchemaObject schemaObject, string columnName = null)
		{
			// get the indexes and constraints on a table
			// NOTE: we don't script system named indexes because we assume they are specified as part of the table definition
			// NOTE: order by type: do the clustered indexes first because they also drop nonclustered indexes if the object is a view (not a table)

			// determine the ID of the table that we are working with
			int tableID;
			if (schemaObject.SchemaObjectType == SchemaObjectType.Index)
				tableID = _connection.ExecuteScalarSql<int>("SELECT i.object_id FROM sys.indexes i WHERE i.name = @ObjectName", new { ObjectName = SqlParser.UnformatSqlName(schemaObject.Name) });
			else
				tableID = _connection.ExecuteScalarSql<int>("SELECT OBJECT_ID(@ObjectName)", new { ObjectName = schemaObject.Name });

			// generate some sql to determine the proper index
			string sql = "SELECT Name=i.name, TableName=o.name, Type=i.type_desc, IsUnique=i.is_unique, IsConstraint=CONVERT(bit, CASE WHEN k.object_id IS NOT NULL THEN 1 ELSE 0 END), IsPrimaryKey=CONVERT(bit, CASE WHEN k.type_desc = 'PRIMARY_KEY_CONSTRAINT' THEN 1 ELSE 0 END), DataSpace=";
			sql += context.IsAzure ? "NULL" : "ISNULL(f.name, p.name)";
			sql += @" FROM sys.indexes i
						JOIN sys.objects o ON (i.object_id = o.object_id)
						LEFT JOIN sys.key_constraints k ON (o.object_id = k.parent_object_id AND i.index_id = k.unique_index_id AND is_system_named = 0)";
			if (!context.IsAzure)
				sql += @"LEFT JOIN sys.partition_schemes p ON (i.data_space_id = p.data_space_id) LEFT JOIN sys.filegroups f ON (i.data_space_id = f.data_space_id)";
			sql += @" WHERE o.object_id = @ObjectID AND i.Name IS NOT NULL";
			if (columnName != null)
				sql += @" AND i.index_Id IN (SELECT index_id 
								FROM sys.index_columns ic
								JOIN sys.columns c ON (c.object_id = ic.object_id AND c.column_id = ic.column_id)
								WHERE ic.object_id = @ObjectID AND c.name = @ColumnName)";
			sql += @" ORDER BY Type";

			// find the indexes on the table
			var indexes = _connection.QuerySql(sql, new { ObjectID = tableID, ColumnName = columnName });
			foreach (dynamic index in indexes)
			{
				// get the columns in the key from the database
				var columns = _connection.QuerySql(@"SELECT ColumnName=c.name
					FROM sys.indexes i
					JOIN sys.index_columns ic ON (i.object_id = ic.object_id AND i.index_id = ic.index_id)
					JOIN sys.columns c ON (ic.object_id = c.object_id AND ic.column_id = c.column_id)
					WHERE i.name = @IndexName",
					new { IndexName = index.Name });

				StringBuilder sb = new StringBuilder();
				if (index.IsConstraint)
				{
					sb.AppendFormat("ALTER TABLE {3} ADD CONSTRAINT {2} {0}{1} (",
						index.IsPrimaryKey ? "PRIMARY KEY " : index.IsUnique ? "UNIQUE " : "",
						index.Type,
						SqlParser.FormatSqlName(SqlParser.IndexNameFromFullName(index.Name)),
						SqlParser.FormatSqlName(index.TableName));
				}
				else
				{
					sb.AppendFormat("CREATE {0}{1} INDEX {2} ON {3} (",
						index.IsUnique ? "UNIQUE " : "",
						index.Type,
						SqlParser.FormatSqlName(SqlParser.IndexNameFromFullName(index.Name)),
						SqlParser.FormatSqlName(index.TableName));
				}
				sb.Append(String.Join(",", columns.Select((dynamic c) => SqlParser.FormatSqlName(c.ColumnName))));
				sb.Append(")");
				if (index.DataSpace != null)
					sb.AppendFormat(" ON {0}", SqlParser.FormatSqlName(index.DataSpace));

				var dropObject = new SchemaObject(sb.ToString());

				ScriptUpdate(context, dropObject);
			}
		}

		/// <summary>
		/// Script the Xml Indexes on a table.
		/// </summary>
		/// <param name="context">The installation context.</param>
		/// <param name="schemaObject">The object to script.</param>
		private void ScriptXmlIndexes(InstallContext context, SchemaObject schemaObject)
		{
			IList<FastExpando> xmlIndexes;

			if (schemaObject.SchemaObjectType == SchemaObjectType.PrimaryXmlIndex)
			{
				// find any secondary indexes dependent upon the primary index
				xmlIndexes = _connection.QuerySql(@"
						IF NOT EXISTS (SELECT * FROM sys.system_objects WHERE name = 'xml_indexes') SELECT TOP 0 Nothing=NULL ELSE
						SELECT Name=i.name, TableName=o.name, SecondaryType=i.secondary_type_desc, ParentIndexName=@ObjectName
						FROM sys.xml_indexes i
						JOIN sys.objects o ON (i.object_id = o.object_id)
						JOIN sys.xml_indexes p ON (p.index_id = i.using_xml_index_id)
						WHERE p.name = @ObjectName",
					new { ObjectName = SqlParser.IndexNameFromFullName(schemaObject.Name) });
			}
			else
			{
				// for tables and primary keys, look for primary xml indexes
				xmlIndexes = _connection.QuerySql(@"
						IF NOT EXISTS (SELECT * FROM sys.system_objects WHERE name = 'xml_indexes') SELECT TOP 0 Nothing=NULL ELSE
						SELECT Name=i.name, TableName=o.name, SecondaryType=i.secondary_type_desc, ParentIndexName=u.name
						FROM sys.xml_indexes i
						JOIN sys.objects o ON (i.object_id = o.object_id)
						LEFT JOIN sys.xml_indexes u ON (i.using_xml_index_id = u.index_id)
						WHERE i.object_id = OBJECT_ID(@ObjectName)",
					new { ObjectName = SqlParser.TableNameFromIndexName(schemaObject.Name) });
			}

			foreach (dynamic xmlIndex in xmlIndexes)
			{
				// get the columns in the key from the database
				var columns = _connection.QuerySql(@"SELECT ColumnName=c.name
					FROM sys.xml_indexes i
					JOIN sys.index_columns ic ON (i.object_id = ic.object_id AND i.index_id = ic.index_id)
					JOIN sys.columns c ON (ic.object_id = c.object_id AND ic.column_id = c.column_id)
					WHERE i.name = @IndexName",
					new { IndexName = xmlIndex.Name });

				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("CREATE {0}XML INDEX {1} ON {2} (",
					(xmlIndex.ParentIndexName == null) ? "PRIMARY " : "",
					SqlParser.FormatSqlName(SqlParser.IndexNameFromFullName(xmlIndex.Name)),
					SqlParser.FormatSqlName(xmlIndex.TableName));
				sb.Append(String.Join(",", columns.Select((dynamic c) => SqlParser.FormatSqlName(c.ColumnName))));
				sb.Append(")");
				if (xmlIndex.SecondaryType != null)
				{
					sb.AppendFormat(" USING XML INDEX {0} FOR ", xmlIndex.ParentIndexName);
					sb.Append(xmlIndex.SecondaryType);
				}

				var dropObject = new SchemaObject(sb.ToString());

				ScriptUpdate(context, dropObject);
			}
		}
		#endregion

		#region Execution Methods
		/// <summary>
		/// Add all of the objects that need to be added.
		/// </summary>
		/// <param name="addObjects">The objects to add.</param>
		/// <param name="objects">The entire schema. Needed for AutoProcs.</param>
		private void AddObjects(List<SchemaObject> addObjects, IEnumerable<SchemaObject> objects)
		{
			// create objects
			foreach (SchemaObject schemaObject in addObjects)
			{
				if (CreatingObject != null)
					CreatingObject(this, new SchemaEventArgs(SchemaEventType.BeforeCreate, schemaObject));

				schemaObject.Install(_connection, objects);

				if (CreatedObject != null)
					CreatedObject(this, new SchemaEventArgs(SchemaEventType.AfterCreate, schemaObject));
			}
		}

		/// <summary>
		/// Drop objects that need to be dropped.
		/// </summary>
		/// <param name="dropObjects">The list of objects to drop.</param>
		private void DropObjects(IEnumerable<SchemaRegistryEntry> dropObjects)
        {
            // drop objects
            foreach (var dropObject in dropObjects)
            {
                if (DroppingObject != null)
					DroppingObject(this, new SchemaEventArgs(SchemaEventType.BeforeDrop, dropObject.ObjectName));

				SchemaObject.Drop(_connection, dropObject.Type, dropObject.ObjectName);
            }
        }

		/// <summary>
		/// Verify that all of the objects that are supposed to be there really are...
		/// </summary>
		/// <param name="schemaObjects">The objects</param>
		private void VerifyObjects (List<SchemaObject> schemaObjects)
		{
			foreach (SchemaObject schemaObject in schemaObjects)
				if (!schemaObject.Verify(_connection))
					throw new SchemaException(String.Format(CultureInfo.InvariantCulture, "Schema Object {0} was not in the database", schemaObject.Name));
		}
		#endregion

		#region Event Handling
        /// <summary>
        /// Called before a SchemaObject is created
        /// </summary>
        public event EventHandler<SchemaEventArgs> CreatingObject;

        /// <summary>
        /// Called after a SchemaObject is created
        /// </summary>
        public event EventHandler<SchemaEventArgs> CreatedObject;

        /// <summary>
        /// Called before a SchemaObject is dropped
        /// </summary>
        public event EventHandler<SchemaEventArgs> DroppingObject;
		#endregion

        #region Internal Helper Methods
		/// Get the objects in the order that they need to be created.
		/// </summary>
		/// <param name="schema">The schema to sort.</param>
		/// <returns>The schema objects in the order that they need to be created.</returns>
		private static List<SchemaObject> OrderSchemaObjects(IEnumerable<SchemaObject> schema)
		{
			// get the list of objects
			List<SchemaObject> schemaObjects = schema.ToList();
			for (int i = 0; i < schemaObjects.Count; i++)
				schemaObjects[i].OriginalOrder = i;

			// sort the list of objects in installation order
			// first compare by type, then original order, then by name
			schemaObjects.Sort(CompareByInstallOrder);

			return schemaObjects;
		}

		/// <summary>
		/// Compares two registry entry objects to determine the appropriate installation order.
		/// </summary>
		/// <param name="e1">The first object to compare.</param>
		/// <param name="e2">The second object to compere.</param>
		/// <returns>The comparison result.</returns>
		private static int CompareByInstallOrder(SchemaRegistryEntry e1, SchemaRegistryEntry e2)
		{
			int compare = e1.Type.CompareTo(e2.Type);
			if (compare == 0)
				compare = e1.OriginalOrder.CompareTo(e2.OriginalOrder);
			if (compare == 0)
				compare = String.Compare(e1.ObjectName, e2.ObjectName, StringComparison.OrdinalIgnoreCase);

			return compare;
		}

		/// <summary>
		/// Compares two schema objects to determine the appropriate installation order.
		/// </summary>
		/// <param name="o1">The first object to compare.</param>
		/// <param name="o2">The second object to compere.</param>
		/// <returns>The comparison result.</returns>
		private static int CompareByInstallOrder(SchemaObject o1, SchemaObject o2)
		{
			int compare = o1.SchemaObjectType.CompareTo(o2.SchemaObjectType);
			if (compare == 0)
				compare = o1.OriginalOrder.CompareTo(o2.OriginalOrder);
			if (compare == 0)
				compare = String.Compare(o1.Name, o2.Name, StringComparison.OrdinalIgnoreCase);
			return compare;
		}
		#endregion

		#region Private Members
		/// <summary>
		/// The current connection to the database
		/// </summary>
		private RecordingDbConnection _connection;
		#endregion

		class InstallContext
		{
			public SchemaRegistry SchemaRegistry;
			public List<SchemaObject> SchemaObjects;
			public List<SchemaRegistryEntry> DropObjects;
			public List<SchemaObject> AddObjects;
			public bool IsAzure;
		}
	}
    #endregion
}