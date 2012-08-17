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
        public SchemaObject (SchemaObjectType type, string name, string sql)
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
        /// The name without formatting pieces
        /// </summary>
        /// <value></value>
        internal string UnformattedName { get { return SqlParser.UnformatSqlName (_name); } }

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
		internal void Install(IDbConnection connection, IEnumerable<SchemaObject> objects)
        {
			string sql = Sql;

			if (SchemaObjectType == Schema.SchemaObjectType.AutoProc)
				sql = new AutoProc(Name, new SqlColumnDefinitionProvider(connection), objects).Sql;

			if (sql.Length > 0)
            {
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
        }
		private static Regex _goSplit = new Regex (@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Multiline);

		internal bool Verify(RecordingDbConnection connection)
		{
			return connection.DoNotLog(() => {
            IDbCommand command = connection.CreateCommand();

			switch (SchemaObjectType)
			{
				default:
				case SchemaObjectType.UserPreScript:
				case SchemaObjectType.Unused:
				case SchemaObjectType.Script:
				case SchemaObjectType.UserScript:
					return true;

				case SchemaObjectType.Permission:
					Match m = Regex.Match(_name, String.Format(CultureInfo.InvariantCulture, @"(?<permission>\w+)\s+ON\s+(?<object>{0})\s+TO\s+(?<user>{0})", SqlParser.SqlNameExpression));

					var permissions = connection.QuerySql<string>(@"SELECT p.permission_name 
							FROM sys.database_principals u
							JOIN sys.database_permissions p ON (u.principal_id = p.grantee_principal_id)
							LEFT JOIN sys.objects o ON (p.class_desc = 'OBJECT_OR_COLUMN' AND p.major_id = o.object_id)
							LEFT JOIN sys.types t ON (p.class_desc = 'TYPE' AND p.major_id = t.user_type_id)
							WHERE u.name = @UserName AND ISNULL(o.name, t.name) = @ObjectName",
							new 
							{ 
								UserName = SqlParser.UnformatSqlName(m.Groups["user"].Value),
								ObjectName = SqlParser.UnformatSqlName(SqlParser.IndexNameFromFullName(m.Groups["object"].Value))
							});

					string permission = m.Groups["permission"].Value.ToUpperInvariant();
					switch (permission)
					{
						case "EXEC":
							return permissions.Contains("EXECUTE");

						case "ALL":
							return permissions.Any();
					}

					if (!permissions.Contains(permission))
						Console.WriteLine("foo");


					return permissions.Contains(permission);

				case SchemaObjectType.Role:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.database_principals WHERE name = '{0}' AND type = 'R'", SqlParser.UnformatSqlName(Regex.Match(Name, @"ROLE (?<name>.*)").Groups["name"].Value));
					break;

				case SchemaObjectType.User:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.database_principals WHERE name = '{0}' AND type = 'U'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Login:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.server_principals WHERE name = '{0}' AND (type = 'U' OR type = 'S')", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Schema:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.schemas WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Certificate:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.certificates WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.MasterKey:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.symmetric_keys WHERE name = '{0}'", "##MS_DatabaseMasterKey##");
					break;

				case SchemaObjectType.SymmetricKey:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.symmetric_keys WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Service:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.services WHERE name = '{0}'", SqlParser.UnformatSqlName(Regex.Match(Name, @"SERVICE (?<name>.*)").Groups["name"].Value));
					break;

				case SchemaObjectType.Queue:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_queues WHERE name = '{0}'", SqlParser.UnformatSqlName(Regex.Match(Name, @"QUEUE (?<name>.*)").Groups["name"].Value));
					break;

				case SchemaObjectType.UserDefinedType:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.types WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.PartitionFunction:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.partition_functions WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.PartitionScheme:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.partition_schemes WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Table:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.tables WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.IndexedView:
				case SchemaObjectType.View:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.views WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.AutoProc:
					return new AutoProc(Name, null, null).GetProcs().All(tuple =>
					{
						int count;
						switch (tuple.Item1)
						{
							case AutoProc.ProcTypes.Table:
							case AutoProc.ProcTypes.IdTable:
								count = connection.ExecuteScalarSql<int>("SELECT COUNT (*) FROM sys.types WHERE name = @ProcName", new { ProcName = SqlParser.UnformatSqlName(tuple.Item2) });
								break;

							default:
								count = connection.ExecuteScalarSql<int>("SELECT COUNT (*) FROM sys.objects WHERE name = @ProcName", new { ProcName = SqlParser.UnformatSqlName(tuple.Item2) });
								break;
						}
						return count > 0;
					});

				case SchemaObjectType.StoredProcedure:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.procedures WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.PrimaryKey:
				case SchemaObjectType.Index:
				case SchemaObjectType.PrimaryXmlIndex:
				case SchemaObjectType.SecondaryXmlIndex:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.indexes WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Trigger:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.triggers WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.ForeignKey:
				case SchemaObjectType.Constraint:
				case SchemaObjectType.Function:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.objects WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.MessageType:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_message_types WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.Contract:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_contracts WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;

				case SchemaObjectType.BrokerPriority:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.conversation_priorities WHERE name = '{0}'", SqlParser.UnformatSqlName(Name));
					break;
			}

			// execute the query
			return (int)command.ExecuteScalar() > 0;

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
			IDbCommand command = connection.CreateCommand();

            string[] split;
            string tableName;
            switch (type)
            {
				default:
					// don't need to drop it (e.g. scripts)
					return;

                case SchemaObjectType.Table:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP TABLE {0}", objectName);
                    break;
                case SchemaObjectType.UserDefinedType:
                    command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP TYPE {0}", objectName);
                    break;
				case SchemaObjectType.MasterKey:
					// don't drop as this could be a loss of data
					command.CommandText = "SELECT NULL";
					break;
				case SchemaObjectType.Certificate:
					// don't drop as this could be a loss of data
					command.CommandText = "SELECT NULL";
					break;
				case SchemaObjectType.SymmetricKey:
					// don't drop as this could be a loss of data
					command.CommandText = "SELECT NULL";
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
				case SchemaObjectType.UserScript:
				case SchemaObjectType.UserPreScript:
					// can't clean up user scripts
					command.CommandText = "SELECT NULL";
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

			try
			{
				foreach (string sql in command.CommandText.Split(new string[] { "GO" }, StringSplitOptions.RemoveEmptyEntries))
					connection.ExecuteSql(sql);
			}
			catch (SqlException e)
			{
				Console.WriteLine ("WARNING: {0}", e.Message);
				//throw;
			}
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
            SHA1Managed shHash = new SHA1Managed ();

            // Create the hash value from the array of bytes.
            byte[] hashValue = shHash.ComputeHash (bytes);

            // return the hash as a string
            return Convert.ToBase64String (hashValue);
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

			// fill in the type and name
			_type = match.SchemaObjectType;
			_name = match.Name;
		}
		#endregion
	}
    #endregion
}
