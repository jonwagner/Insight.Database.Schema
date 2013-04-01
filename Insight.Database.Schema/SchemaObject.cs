#region Using directives

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Insight.Database;
#endregion

namespace Insight.Database.Schema
{
    #region SchemaObject Class
    public sealed class SchemaObject
    {
        #region Constructors
        /// <summary>
        /// Constructs a SchemaObject of the given type, name, and sql script
        /// </summary>
        /// <param name="type">The type of the SchemaObject</param>
        /// <param name="name">The name of the SchemaObject</param>
        /// <param name="sql">The SQL script for the SchemaObject</param>
        /// <remarks>The type and name must match the SQL script.</remarks>
        /// <exception cref="ArgumentNullException">If name or sql is null</exception>
        internal SchemaObject (SchemaObjectType type, string name, string sql)
        {
            if (name == null) throw new ArgumentNullException ("name");
            if (sql == null) throw new ArgumentNullException ("sql");


            _type = type;
            _name = name;
            _sql = sql;
        }

        /// <summary>
        /// Create a SchemaObject from a sql snippet, attempting to detect the type and name of the object
        /// </summary>
        /// <param name="sql">The sql to parse</param>
        /// <exception cref="SchemaParsingException">If the SQL cannot be parsed</exception>
        public SchemaObject (string sql)
        {
			_sql = sql;
            ParseSql ();
        }
        #endregion

        #region Properties
        /// <summary>
        /// The sql script for the object
        /// </summary>
        /// <value>The sql script for the object</value>
        /// <exception cref="SqlParsingException">If sql cannot be parsed</exception>
        public string Sql 
        { 
            get { return _sql; } 
            set 
            {
                if (value == null) throw new ArgumentNullException ("value");
                _sql = value; 
                ParseSql (); 
            } 
        }
        private string _sql;

        /// <summary>
        /// The name of the SchemaObject
        /// </summary>
        /// <value>The name of the SchemaObject</value>
        public string Name { get { return _name; } }
        private string _name;

        /// <summary>
        /// The type of the SchemaObject
        /// </summary>
        /// <value>The type of the SchemaObject</value>
        public SchemaObjectType SchemaObjectType { get { return _type; } }
        private SchemaObjectType _type;

        /// <summary>
        /// The signature of the SchemaObject
        /// </summary>
        /// <value></value>
		internal string GetSignature(IDbConnection connection, IEnumerable<SchemaObject> objects)
		{
			if (_type == Schema.SchemaObjectType.AutoProc)
				return new AutoProc(_name, new SqlColumnDefinitionProvider(connection), objects).Signature;
			else
				return CalculateSignature(Sql);
		}

        /// <summary>
        /// The order in which the script was added to the schema
        /// </summary>
        /// <value>The original order</value>
        /// <remarks>Used for ties within the same type of object</remarks>
        internal int OriginalOrder
        {
            get { return _originalOrder; }
            set { _originalOrder = value; }
        }
        private int _originalOrder;
        #endregion

        #region Install/Uninstall Methods
        /// <summary>
        /// Install the object into the database
        /// </summary>
        /// <param name="connection">The database connection to use</param>
		internal void Install(RecordingDbConnection connection, IEnumerable<SchemaObject> objects)
        {
			string sql = Sql;

            // for auto-procs, convert the comment into a list of stored procedures
			if (SchemaObjectType == Schema.SchemaObjectType.AutoProc)
			{
				sql = new AutoProc(Name, new SqlColumnDefinitionProvider(connection), objects).Sql;
				if (sql.Length == 0)
					return;
			}
			
			try
			{
				foreach (string s in _goSplit.Split(sql).Where(piece => !String.IsNullOrWhiteSpace(piece)))
					connection.ExecuteSql(s);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(String.Format(CultureInfo.InvariantCulture, "Cannot create SQL object {0}: {1}", Name, e.Message), e);
			}
        }

        /// <summary>
        /// Determines how to split a GO statement in a batch.
        /// </summary>
		private static Regex _goSplit = new Regex (@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Multiline);

        /// <summary>
        /// Verify that the object exists in the database.
        /// </summary>
        /// <param name="connection">The connection to query.</param>
        /// <returns>True if the object exists, false if it doesn't.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1505:AvoidUnmaintainableCode"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
		internal bool Exists(RecordingDbConnection connection)
		{
			return connection.DoNotLog(() => {

                string command;

			    switch (SchemaObjectType)
			    {
				    default:
				    case SchemaObjectType.UserPreScript:
				    case SchemaObjectType.Unused:
				    case SchemaObjectType.Script:
				    case SchemaObjectType.UserScript:
                        // we can't check these
					    return true;

				    case SchemaObjectType.Permission:
                        // check permissions by querying the permissions table
					    Match m = Regex.Match(_name, String.Format(CultureInfo.InvariantCulture, @"(?<permission>\w+)\s+ON\s+(?<object>{0})\s+TO\s+(?<user>{0})", SqlParser.SqlNameExpression));

						var permissions = connection.QuerySql(@"SELECT PermissionName=p.permission_name, ObjectType=ISNULL(o.type_desc, p.class_desc)
							    FROM sys.database_principals u
							    JOIN sys.database_permissions p ON (u.principal_id = p.grantee_principal_id)
							    LEFT JOIN sys.objects o ON (p.class_desc = 'OBJECT_OR_COLUMN' AND p.major_id = o.object_id)
							    LEFT JOIN sys.types t ON (p.class_desc = 'TYPE' AND p.major_id = t.user_type_id)
							    LEFT JOIN sys.schemas s ON (p.class_desc = 'SCHEMA' AND p.major_id = s.schema_id)
							    WHERE state_desc IN ('GRANT', 'GRANT_WITH_GRANT_OPTION') AND u.name = @UserName AND COALESCE(o.name, t.name, s.name) = @ObjectName",
								new Dictionary<string, object>()
								{ 
									{ "UserName", SqlParser.UnformatSqlName(m.Groups["user"].Value) },
									{ "ObjectName", SqlParser.UnformatSqlName(SqlParser.IndexNameFromFullName(m.Groups["object"].Value)) }
								});

						string type = permissions.Select((dynamic p) => p.ObjectType).FirstOrDefault();
					    string permission = m.Groups["permission"].Value.ToUpperInvariant();

					    switch (permission)
					    {
						    case "EXEC":
							    return permissions.Any((dynamic p) => p.PermissionName == "EXECUTE");

						    case "ALL":
								switch (type)
								{
									case null:
										// this happens on initial install when there is no database
										return false;

									case "SQL_STORED_PROCEDURE":
										return permissions.Any((dynamic p) => p.PermissionName == "EXECUTE");

									case "SQL_SCALAR_FUNCTION":
										return permissions.Any((dynamic p) => p.PermissionName == "EXECUTE") &&
											permissions.Any((dynamic p) => p.PermissionName == "REFERENCES");

									case "SQL_INLINE_TABLE_VALUED_FUNCTION":
									case "SQL_TABLE_VALUED_FUNCTION":
									case "USER_TABLE":
									case "VIEW":
										return permissions.Any((dynamic p) => p.PermissionName == "REFERENCES") &&
											permissions.Any((dynamic p) => p.PermissionName == "SELECT") &&
											permissions.Any((dynamic p) => p.PermissionName == "INSERT") &&
											permissions.Any((dynamic p) => p.PermissionName == "UPDATE") &&
											permissions.Any((dynamic p) => p.PermissionName == "DELETE");

									case "DATABASE":
										return permissions.Any((dynamic p) => p.PermissionName == "BACKUP DATABASE") &&
											permissions.Any((dynamic p) => p.PermissionName == "BACKUP LOG") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE DATABASE") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE DEFAULT") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE FUNCTION") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE PROCEDURE") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE RULE") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE TABLE") &&
											permissions.Any((dynamic p) => p.PermissionName == "CREATE VIEW");

									default:
										throw new SchemaException(String.Format(CultureInfo.InvariantCulture, "GRANT ALL is not supported for {0} objects", type));
								}

							default:
								return permissions.Any((dynamic p) => p.PermissionName == permission);
						}

				    case SchemaObjectType.Role:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.database_principals WHERE name = '{0}' AND type = 'R'", SqlParser.UnformatSqlName(Regex.Match(Name, @"ROLE (?<name>.*)").Groups["name"].Value));
					    break;

				    case SchemaObjectType.User:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.database_principals WHERE name = '{0}' AND type = 'U'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.Login:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.server_principals WHERE name = '{0}' AND (type = 'U' OR type = 'S')", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.Schema:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.schemas WHERE name = '{0}'", SqlParser.UnformatSqlName(Regex.Match(Name, @"SCHEMA (?<name>.*)").Groups["name"].Value));
					    break;

				    case SchemaObjectType.Certificate:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.certificates WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.MasterKey:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.symmetric_keys WHERE name = '{0}'", "##MS_DatabaseMasterKey##");
					    break;

				    case SchemaObjectType.SymmetricKey:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.symmetric_keys WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.Service:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.services WHERE name = '{0}'", SqlParser.UnformatSqlName(Regex.Match(Name, @"SERVICE (?<name>.*)").Groups["name"].Value));
					    break;

				    case SchemaObjectType.Queue:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_queues WHERE name = '{0}'", SqlParser.UnformatSqlName(Regex.Match(Name, @"QUEUE (?<name>.*)").Groups["name"].Value));
					    break;

				    case SchemaObjectType.UserDefinedType:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.types WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.PartitionFunction:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.partition_functions WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.PartitionScheme:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.partition_schemes WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.Table:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.tables WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.IndexedView:
				    case SchemaObjectType.View:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.views WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.AutoProc:
					    return new AutoProc(Name, null, null).GetProcs().All(tuple =>
					    {
						    int count;
						    switch (tuple.Item1)
						    {
							    case AutoProc.ProcTypes.Table:
							    case AutoProc.ProcTypes.IdTable:
									count = connection.ExecuteScalarSql<int>("SELECT COUNT (*) FROM sys.types WHERE name = @Name", new Dictionary<string, object>() { { "Name", SqlParser.UnformatSqlName(tuple.Item2) } });
								    break;

							    default:
									count = connection.ExecuteScalarSql<int>("SELECT COUNT (*) FROM sys.objects WHERE name = @Name", new Dictionary<string, object>() { { "Name", SqlParser.UnformatSqlName(tuple.Item2) } });
								    break;
						    }
						    return count > 0;
					    });

				    case SchemaObjectType.StoredProcedure:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.procedures WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.PrimaryKey:
				    case SchemaObjectType.Index:
				    case SchemaObjectType.PrimaryXmlIndex:
				    case SchemaObjectType.SecondaryXmlIndex:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.indexes WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.Trigger:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.triggers WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.ForeignKey:
				    case SchemaObjectType.Constraint:
				    case SchemaObjectType.Function:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.objects WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

					case SchemaObjectType.Default:
						{
							command = String.Format(CultureInfo.InvariantCulture, @"SELECT COUNT(*)
							FROM sys.default_constraints d
							JOIN sys.objects o ON (d.parent_object_id = o.object_id)
							JOIN sys.columns c ON (c.object_id = o.object_id AND c.column_id = d.parent_column_id)
							WHERE o.name = '{0}' AND c.name = '{1}'", SqlParser.TableNameFromIndexName(Name), SqlParser.IndexNameFromFullName(Name));
						}
						break;

				    case SchemaObjectType.MessageType:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_message_types WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.Contract:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_contracts WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;

				    case SchemaObjectType.BrokerPriority:
					    command = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.conversation_priorities WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					    break;
			    }

			    // execute the query
			    return (int)connection.ExecuteScalarSql<int>(command, null) > 0;
			});
		}

		/// <summary>
		/// Determine if this is a type of object that we can drop.
		/// </summary>
		/// <param name="type">The type of the object.</param>
		/// <returns>True if we know how to drop the object.</returns>
		internal static bool CanDrop(SchemaObjectType type)
		{
			switch (type)
			{
				case SchemaObjectType.MasterKey:
				case SchemaObjectType.Certificate:
				case SchemaObjectType.SymmetricKey:
					// don't drop as this could be a loss of data
					return false;

				case SchemaObjectType.UserScript:
				case SchemaObjectType.UserPreScript:
					// can't clean up user scripts
					return false;
			}

			return true;
		}

		/// <summary>
		/// Determine if this is a type of object that we can modify.
		/// </summary>
		/// <param name="type">The type of the object.</param>
		/// <returns>True if we know how to drop the object.</returns>
		internal bool CanModify(SchemaInstaller.InstallContext context, RecordingDbConnection connection)
		{
			// if we don't know how to drop it, then we can't modify it
			if (!CanDrop(SchemaObjectType))
				return false;

			return connection.DoNotLog(() =>
			{
				switch (SchemaObjectType)
				{
					case SchemaObjectType.UserDefinedType:
						// we can drop a udt unless it is used in a table
						return connection.ExecuteScalarSql<int>("SELECT COUNT(*) FROM sys.types t JOIN sys.columns c ON (t.user_type_id = c.user_type_id) WHERE t.Name = @Name", new Dictionary<string, object>() { { "Name", SqlParser.UnformatSqlName(Name) } }) == 0;

					case SchemaObjectType.PartitionFunction:
						// we can drop a function as long as there are no schemes using it
						return connection.ExecuteScalarSql<int>("SELECT COUNT(*) FROM sys.partition_functions p JOIN sys.partition_schemes s ON (p.function_id = s.function_id) WHERE p.name = @Name", new Dictionary<string, object>() { { "Name", SqlParser.UnformatSqlName(Name) } }) == 0;

					case SchemaObjectType.PartitionScheme:
						// we can drop a scheme as long as there are no schemes using it
						return connection.ExecuteScalarSql<int>(@"SELECT COUNT(*) FROM sys.partition_schemes s 
							WHERE s.name = @Name AND (
								s.data_space_id IN (SELECT data_space_id FROM sys.indexes) OR 
								s.data_space_id IN (SELECT lob_data_space_id FROM sys.tables) OR
								s.data_space_id IN (SELECT filestream_data_space_id FROM sys.tables))
							", new Dictionary<string, object>() { { "Name", SqlParser.UnformatSqlName(Name) } }) == 0;

					case Schema.SchemaObjectType.Index:
					case Schema.SchemaObjectType.PrimaryKey:
						// azure can't drop the clustered index, so we have to warn if we are attempting to modify that
						if (context.IsAzure)
						{
							if (Sql.IndexOf("NONCLUSTERED", StringComparison.OrdinalIgnoreCase) == -1 && Sql.IndexOf("CLUSTERED", StringComparison.OrdinalIgnoreCase) != -1)
								return false;
						}
						break;
				}

				// everything else we can handle
				return true;
			});
		}

        /// <summary>
        /// Drop an object from the database
        /// </summary>
        /// <param name="connection">The Sql connection to use</param>
        /// <param name="type">The type of the object</param>
        /// <param name="objectName">The name of the object</param>
		internal static void Drop(IDbConnection connection, SchemaObjectType type, string objectName)
        {
			// if this is not a type we can drop then don't
			if (!CanDrop(type))
				return;

			IDbCommand command = connection.CreateCommand();

            string[] split;
            string tableName;
            switch (type)
            {
                case SchemaObjectType.Table:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP TABLE {0}", objectName);
                    break;

                case SchemaObjectType.UserDefinedType:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP TYPE {0}", objectName);
                    break;

				case SchemaObjectType.Index:
                case SchemaObjectType.PrimaryXmlIndex:
                case SchemaObjectType.SecondaryXmlIndex:
                    split = objectName.Split ('.');
                    tableName = split[split.Length - 2];
                    string indexName = split[split.Length - 1];
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP INDEX {1} ON {0}", tableName, indexName);
                    break;

                case SchemaObjectType.PrimaryKey:
                case SchemaObjectType.Constraint:
                case SchemaObjectType.ForeignKey:
                    // need to drop constraints by table and constraint name
                    split = objectName.Split ('.');
                    tableName = split[split.Length - 2];
                    string constraintName = split[split.Length - 1];
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, constraintName);
                    break;

				case SchemaObjectType.Default:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, @"
						-- ALTER TABLE DROP DEFAULT ON COLUMN
						DECLARE @Name[nvarchar](256) 
						SELECT @Name = d.name FROM sys.default_constraints d
							JOIN sys.objects o ON (d.parent_object_id = o.object_id)
							JOIN sys.columns c ON (c.object_id = o.object_id AND c.column_id = d.parent_column_id)
							WHERE o.name = '{1}' AND c.name = '{2}'
						DECLARE @sql[nvarchar](MAX) = 'ALTER TABLE {0} DROP CONSTRAINT [' + @Name + ']'
						EXEC sp_executesql @sql
					",
					 SqlParser.FormatSqlName(SqlParser.TableNameFromIndexName(objectName)),
					 SqlParser.TableNameFromIndexName(objectName), 
					 SqlParser.IndexNameFromFullName(objectName));
					break;

				case SchemaObjectType.IndexedView:
				case SchemaObjectType.View:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP VIEW {0}", objectName);
                    break;

                case SchemaObjectType.Function:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP FUNCTION {0}", objectName);
                    break;

                case SchemaObjectType.StoredProcedure:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP PROCEDURE {0}", objectName);
                    break;

                case SchemaObjectType.Permission:
                    // revoke a permission by replacing GRANT with REVOKE in the name
                    command.CommandText = "REVOKE " + objectName;
                    break;

                case SchemaObjectType.Trigger:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP TRIGGER {0}", objectName);
                    break;

                case SchemaObjectType.User:
                case SchemaObjectType.Login:
                case SchemaObjectType.Schema:
                case SchemaObjectType.Role:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP {0}", objectName);
                    break;

				case SchemaObjectType.PartitionScheme:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP PARTITION SCHEME {0}", objectName);
					break;

				case SchemaObjectType.PartitionFunction:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP PARTITION FUNCTION {0}", objectName);
					break;

				case SchemaObjectType.Queue:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP QUEUE {0}", SqlParser.UnformatSqlName (objectName).Split (new char[] {' '}, 2) [1]);
					break;

				case SchemaObjectType.Service:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP SERVICE {0}", SqlParser.UnformatSqlName (objectName).Split (new char [] { ' ' }, 2) [1]);
					break;

				case SchemaObjectType.MessageType:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP MESSAGE TYPE {0}", objectName);
					break;

				case SchemaObjectType.Contract:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP CONTRACT {0}", objectName);
					break;

				case SchemaObjectType.BrokerPriority:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP BROKER PRIORITY {0}", objectName);
					break;

				case SchemaObjectType.AutoProc:
					command.CommandText = new AutoProc(objectName, new SqlColumnDefinitionProvider(connection), null).DropSql;
					break;
			}

			foreach (string sql in command.CommandText.Split(new string[] { "GO" }, StringSplitOptions.RemoveEmptyEntries))
				connection.ExecuteSql(sql);
		}
        #endregion

        #region Signature Methods
        /// <summary>
        /// Calculate the signature of a string as a hash code
        /// </summary>
        /// <param name="s">The string to hash</param>
        /// <returns>The hash code for the string</returns>
        /// <remarks>This is used to detect changes to a schema object</remarks>
        internal static string CalculateSignature (string s)
        {
            // Convert the string into an array of bytes.
            byte[] bytes = new UnicodeEncoding ().GetBytes (s);

            // Create a new instance of SHA1Managed to create the hash value.
			using (SHA1Managed shHash = new SHA1Managed())
			{
				// Create the hash value from the array of bytes.
				byte[] hashValue = shHash.ComputeHash(bytes);

				// return the hash as a string
				return Convert.ToBase64String(hashValue);
			}
        }
        #endregion

        #region Parsing Methods
        /// <summary>
        /// Parse the SQL to determine the type and name
        /// </summary>
        private void ParseSql ()
        {
			// find the first match by position, then by type
			var match = SqlParser.Parsers.Select(p => p.Match(_sql))
				.Where(m => m != null)
				.OrderBy(m => m.Position)
				.ThenBy(m => m.SchemaObjectType)
				.FirstOrDefault();

			// if we didn't get a match, then throw an exception
			if (match == null)
				throw new SchemaParsingException (Properties.Resources.CannotDetermineScriptType, _sql);

            // if the sql contains something that we know that we don't support, then throw an exception
            var invalid = SqlParser.UnsupportedSql.Select(p => p.Match(_sql)).Where(m => m != null).FirstOrDefault();
            if (invalid != null)
                throw new SchemaParsingException(String.Format(CultureInfo.InvariantCulture, "Error parsing Sql: {0}", invalid.Name), _sql);

			// fill in the type and name
			_type = match.SchemaObjectType;
			_name = match.Name;
		}
		#endregion
	}
    #endregion
}
