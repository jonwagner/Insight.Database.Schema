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
        internal string UnformattedName { get { return UnformatSqlName (_name); } }

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
		internal string GetSignature(SchemaInstaller installer, IEnumerable<SchemaObject> objects)
		{
			if (_type == Schema.SchemaObjectType.AutoProc)
				return new AutoProc(_name, new SqlColumnDefinitionProvider(installer), objects).Signature;
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
		internal void Install(SchemaInstaller installer, IEnumerable<SchemaObject> objects)
        {
			string sql = Sql;

			if (SchemaObjectType == Schema.SchemaObjectType.AutoProc)
				sql = new AutoProc(Name, new SqlColumnDefinitionProvider(installer), objects).Sql;

			if (sql.Length > 0)
            {
				try
				{
					foreach (string s in _goSplit.Split(sql).Where(piece => !String.IsNullOrWhiteSpace(piece)))
						installer.ExecuteNonQuery (s);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException(String.Format(CultureInfo.InvariantCulture, "Cannot create SQL object {0}: {1}", Name, e.Message), e);
				}
            }
        }
		private static Regex _goSplit = new Regex (@"[\s\b]GO[\s\b]", RegexOptions.IgnoreCase | RegexOptions.Compiled);

		internal void Verify (SchemaInstaller installer, SqlConnection connection, IEnumerable<SchemaObject> objects)
		{
            SqlCommand command = new SqlCommand ();
            command.Connection = connection;
			command.CommandTimeout = 0;

			switch (SchemaObjectType)
			{
				default:
				case SchemaObjectType.UserPreScript:
				case SchemaObjectType.Unused:
				case SchemaObjectType.Script:
				case SchemaObjectType.UserScript:
				case SchemaObjectType.Permission:
					return;

				case SchemaObjectType.Role:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.database_principals WHERE name = '{0}' AND type = 'R'", Regex.Match(Name, @"\[ROLE (?<name>[^\]]*)\]").Groups["name"].Value);
					break;

				case SchemaObjectType.User:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.database_principals WHERE name = '{0}' AND type = 'U'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Login:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.server_principals WHERE name = '{0}' AND (type = 'U' OR type = 'S')", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Schema:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.schemas WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Certificate:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.certificates WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.MasterKey:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.symmetric_keys WHERE name = '{0}'", "##MS_DatabaseMasterKey##");
					break;

				case SchemaObjectType.SymmetricKey:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.symmetric_keys WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Service:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.services WHERE name = '{0}'", Regex.Match(Name, @"\[SERVICE (?<name>[^\]]*)\]").Groups["name"].Value);
					break;

				case SchemaObjectType.Queue:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_queues WHERE name = '{0}'", Regex.Match(Name, @"\[QUEUE (?<name>[^\]]*)\]").Groups["name"].Value);
					break;

				case SchemaObjectType.UserDefinedType:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.types WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.PartitionFunction:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.partition_functions WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.PartitionScheme:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.partition_schemes WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Table:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.tables WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.View:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.views WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.StoredProcedure:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.procedures WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.PrimaryKey:
				case SchemaObjectType.Index:
				case SchemaObjectType.PrimaryXmlIndex:
				case SchemaObjectType.SecondaryXmlIndex:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.indexes WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Trigger:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.triggers WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.ForeignKey:
				case SchemaObjectType.Constraint:
				case SchemaObjectType.Function:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.objects WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.MessageType:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_message_types WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.Contract:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.service_contracts WHERE name = '{0}'", UnformatSqlName(Name));
					break;

				case SchemaObjectType.BrokerPriority:
					command.CommandText = String.Format(CultureInfo.InvariantCulture, "SELECT COUNT (*) FROM sys.conversation_priorities WHERE name = '{0}'", UnformatSqlName(Name));
					break;
			}

			// execute the query
			int exists = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);

			// if it doesn't exist, install it
			if (exists == 0)
			{
				installer.OnMissingObject (this, new SchemaEventArgs (SchemaEventType.MissingObject, this));
				Install(installer, objects);
			}
		}


        /// <summary>
        /// Drop an object from the database
        /// </summary>
        /// <param name="connection">The Sql connection to use</param>
        /// <param name="type">The type of the object</param>
        /// <param name="objectName">The name of the object</param>
		internal static void Drop (SchemaInstaller installer, SqlConnection connection, SchemaObjectType type, string objectName)
        {
            SqlCommand command = new SqlCommand ();
            command.Connection = connection;

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
                    return;
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
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP QUEUE {0}", UnformatSqlName (objectName).Split (new char[] {' '}, 2) [1]);
					break;
				case SchemaObjectType.Service:
					command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP SERVICE {0}", UnformatSqlName (objectName).Split (new char [] { ' ' }, 2) [1]);
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
					command.CommandText = new AutoProc(objectName, new SqlColumnDefinitionProvider(installer), null).DropSql;
					break;
			}

			try
			{
				foreach (string sql in command.CommandText.Split(new string[] { "GO" }, StringSplitOptions.RemoveEmptyEntries))
					installer.ExecuteNonQuery(sql);
			}
			catch (SqlException e)
			{
				Console.WriteLine ("WARNING: {0}", e.Message);
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
        private bool ParseSql ()
        {
			List<SqlParser> matches = new List<SqlParser> ();

            // let all of the parsers try to guess the type
            foreach (SqlParser parser in _parserList)
            {
				if (parser.Match (_sql))
					matches.Add (parser);
            }
			
			// find the earliest match
			matches.Sort (delegate (SqlParser p1, SqlParser p2)
			{
				SchemaObjectType type1;
				int pos1;
				p1.Match (_sql, out type1, out _name, out pos1);

				SchemaObjectType type2;
				int pos2;
				p2.Match (_sql, out type2, out _name, out pos2);

				// stick unused types as the last resort
				if (type1 == SchemaObjectType.Unused)
					return 1;
				if (type2 == SchemaObjectType.Unused)
					return -1;

				int compare = pos1.CompareTo (pos2);
				if (compare == 0)
					compare = type1.CompareTo (type2);

				return compare;
			});

			// we can't determine the type, so we have to quit
			if (matches.Count == 0)
				throw new SchemaParsingException (Properties.Resources.CannotDetermineScriptType, _sql);

			// use the earliest match
			int pos;
			matches [0].Match (_sql, out _type, out _name, out pos);

			// this is just junk stuff that SQL inserts
			if (_type == SchemaObjectType.Unused)
				return false;

			return true;
        }

        /// <summary>
        /// The list of sql parsers
        /// </summary>
        private static SqlParserList _parserList = new SqlParserList();

        #region SqlParserClass
        /// <summary>
        /// A parser that detects the type and name of a sql object from a script
        /// </summary>
        class SqlParser
        {
            /// <summary>
            /// Create a parser that detects a type from a pattern
            /// </summary>
            /// <param name="type">The type represented by the pattern</param>
            /// <param name="pattern">The pattern to detect</param>
            public SqlParser (SchemaObjectType type, string pattern)
            {
                _type = type;
                _regex = new Regex (pattern, RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.ExplicitCapture | RegexOptions.Compiled);
                _result = "$1";
            }

            /// <summary>
            /// Create a parser that detects a type from a pattern
            /// </summary>
            /// <param name="type">The type represented by the pattern</param>
            /// <param name="pattern">The pattern to detect</param>
            /// <param name="result">The string used to generate the resulting name</param>
            public SqlParser (SchemaObjectType type, string pattern, string result)
            {
                _type = type;
                _regex = new Regex (pattern, RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.ExplicitCapture | RegexOptions.Compiled);
                _result = result;
            }

			/// <summary>
            /// Attempts to match the sql to the pattern. If successful, sets the type and name
            /// </summary>
            /// <param name="sql">The sql to parse</param>
            /// <param name="type">If matched, updated to the current type</param>
            /// <param name="name">If matched, updated to the name</param>
            /// <returns></returns>
            public bool Match (string sql, out SchemaObjectType type, out string name, out int position)
            {
                Match match = _regex.Match (sql);
                if (match.Success)
                {
                    type = _type;

					// format the sql name so we don't have to worry about what people type
					name = match.Result (_result);
					if (type != SchemaObjectType.Permission && type != SchemaObjectType.AutoProc)
					{
						string [] pieces = name.Split (_sqlNameDivider);
						name = "";
						for (int i = pieces.Length - _result.Split (_sqlNameDivider).Length; i < pieces.Length; i++)
						{
							if (name.Length > 0)
								name += ".";
							name += FormatSqlName (pieces [i]);
						}
					}

					position = match.Index;
                    return true;
                }

				type = SchemaObjectType.Unused;
				name = null;
				position = -1;

                return false;
            }

			/// <summary>
			/// Attempts to match the sql to the pattern
			/// </summary>
			/// <param name="sql"></param>
			/// <returns></returns>
			public bool Match (string sql)
			{
				return _regex.Match (sql).Success;
			}

			/// <summary>
            /// Compare the parser by type
            /// </summary>
            /// <param name="p1">Parser to compare</param>
            /// <param name="p2">Parser to compare</param>
            /// <returns>The sort order</returns>
            public static int CompareByType (SqlParser p1, SqlParser p2) { return p1._type.CompareTo (p2._type); }

            /// <summary>
            /// The expression pattern to match
            /// </summary>
            private Regex _regex;

            /// <summary>
            /// The corresponding object type
            /// </summary>
            private SchemaObjectType _type;

            /// <summary>
            /// The string used to generate the result from the match
            /// </summary>
            private string _result;
        }
        #endregion

        #region SqlParserList Class
        /// <summary>
        /// The list of SqlParser codes
        /// </summary>
        class SqlParserList : List<SqlParser>
        {
            /// <summary>
            /// Create a list of SqlParsers
            /// </summary>
            public SqlParserList ()
            {
				Add (new SqlParser (SchemaObjectType.IndexedView, String.Format (CultureInfo.InvariantCulture, @"^\s*--\s*INDEXEDVIEW\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.UserPreScript, String.Format (CultureInfo.InvariantCulture, @"^\s*--\s*PRESCRIPT\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.UserScript, String.Format (CultureInfo.InvariantCulture, @"^\s*--\s*SCRIPT\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.UserDefinedType, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+TYPE\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.UserDefinedType, String.Format (CultureInfo.InvariantCulture, @"EXEC(UTE)?\s+sp_addtype\s+'?(?<name>{0})'?", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.MasterKey, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+MASTER\s+KEY\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.Certificate, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+CERTIFICATE\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.SymmetricKey, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+SYMMETRIC\s+KEY\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.PartitionFunction, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+PARTITION\s+FUNCTION\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.PartitionScheme, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+PARTITION\s+SCHEME\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.MessageType, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+MESSAGE TYPE\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.Contract, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+CONTRACT\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.BrokerPriority, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+BROKER\s+PRIORITY\s+(?<name>{0})", SqlNameExpression)));
				Add (new SqlParser (SchemaObjectType.Queue, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+QUEUE\s+(?<name>{0})", SqlNameExpression), "QUEUE $1"));
				Add (new SqlParser (SchemaObjectType.Service, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+SERVICE\s+(?<name>{0})", SqlNameExpression), "SERVICE $1"));
				Add (new SqlParser (SchemaObjectType.Table, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+TABLE\s+(?<name>{0})", SqlNameExpression)));
                Add (new SqlParser (SchemaObjectType.Trigger, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+TRIGGER\s+(?<name>{0})", SqlNameExpression)));
                Add (new SqlParser (SchemaObjectType.Index, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+(UNIQUE\s+)?(((CLUSTERED)|(NONCLUSTERED))\s+)?INDEX\s+(?<indname>{0})\s+ON\s+(?<tablename>{0})", SqlNameExpression), "$2.$1"));
                Add (new SqlParser (SchemaObjectType.View, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+VIEW\s+(?<name>{0})", SqlNameExpression)));
                Add (new SqlParser (SchemaObjectType.StoredProcedure, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+PROC(EDURE)?\s+(?<name>{0})", SqlNameExpression)));
                Add (new SqlParser (SchemaObjectType.Permission, String.Format (CultureInfo.InvariantCulture, @"GRANT\s+(?<permission>{0})\s+ON\s+(?<name>{0})\s+TO\s+(?<grantee>{0})", SqlNameExpression), "$1 ON $2 TO $3"));
				Add(new SqlParser(SchemaObjectType.PrimaryKey, String.Format(CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+(?<tablename>{0})\s+(WITH\s+(NO)?CHECK\s+)?ADD\s+CONSTRAINT\s*\(?(?<name>{0})\)?\s+PRIMARY\s+", SqlNameExpression), "$1.$2"));
				Add(new SqlParser(SchemaObjectType.ForeignKey, String.Format(CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+(?<tablename>{0})\s+(WITH\s+(NO)?CHECK\s+)?ADD\s+CONSTRAINT\s*\(?(?<name>{0})\)?\s+FOREIGN\s+KEY\s*\(?(?<name>{0})\)?", SqlNameExpression), "$1.$2"));
				Add(new SqlParser(SchemaObjectType.Constraint, String.Format(CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+(?<tablename>{0})\s+(WITH\s+(NO)?CHECK\s+)?ADD\s+CONSTRAINT\s*\(?(?<name>{0})\)?", SqlNameExpression), "$1.$2"));
                Add (new SqlParser (SchemaObjectType.Function, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+FUNCTION\s+(?<name>{0})", SqlNameExpression)));
                Add (new SqlParser (SchemaObjectType.PrimaryXmlIndex, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+PRIMARY\s+XML\s+INDEX\s+(?<name>{0})\s+ON\s+(?<tablename>{0})", SqlNameExpression), "$2.$1"));
                Add (new SqlParser (SchemaObjectType.SecondaryXmlIndex, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+XML\s+INDEX\s+(?<name>{0})\s+ON\s+(?<tablename>{0})", SqlNameExpression), "$2.$1"));
                Add (new SqlParser (SchemaObjectType.Login, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+LOGIN\s+(?<name>{0})", SqlNameExpression), "LOGIN $1"));
                Add (new SqlParser (SchemaObjectType.User, String.Format (CultureInfo.InvariantCulture, @"CREATE\s+USER\s+(?<name>{0})", SqlNameExpression), "USER $1"));
				Add (new SqlParser (SchemaObjectType.Role, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+ROLE\s+(?<name>{0})", SqlNameExpression), "ROLE $1"));
				Add (new SqlParser (SchemaObjectType.Schema, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+SCHEMA\s+(?<name>{0})", SqlNameExpression), "SCHEMA $1"));
				Add(new SqlParser(SchemaObjectType.Unused, String.Format(CultureInfo.InvariantCulture, @"SET\s+ANSI_NULLS", SqlNameExpression), null));
				Add(new SqlParser(SchemaObjectType.Unused, String.Format(CultureInfo.InvariantCulture, @"SET\s+QUOTED_IDENTIFIER", SqlNameExpression), null));
				Add(new SqlParser(SchemaObjectType.AutoProc, AutoProc.AutoProcRegex, "$0"));

				Sort(new Comparison<SqlParser>(SqlParser.CompareByType));
            }
        }

        /// <summary>
        /// Matches a SQL name in the form [a].[b].[c], or "a"."b"."c" or a.b.c (or any combination)
		/// Also: TYPE :: sqlname for global scoping
        /// </summary>
		internal const string SqlNameExpression = @"(([\w\d]+\s*::\s*)?((""[^""]+"")|(\[[^\]]+\])|[\w\d]+)\.){0,2}((""[^""]+"")|(\[[^\]]+\])|[\w\d]+)";
        #endregion
        #endregion

        #region Formatting Methods
        /// <summary>
        /// Get the name of a SqlObject without owner and schema, and unformat the name
        /// </summary>
        /// <remarks>[dbo]..[foo] returns foo</remarks>
        /// <param name="name">The full name to clean up</param>
        /// <returns>The unformatted object name</returns>
        internal static string UnformatSqlName (string name)
        {
            string[] splitName = name.Split (_sqlNameDivider);
            string objectName = splitName[splitName.Length - 1];
            return _sqlNameCharactersRegex.Replace (objectName, "");
        }

        /// <summary>
        /// Get the table name from the name of an index
        /// </summary>
        /// <param name="indexName">The name of the index</param>
        /// <returns>The name of the table</returns>
        /// <exception cref="ArgumentException">If the table name cannot be determined</exception>
        internal static string TableNameFromIndexName (string indexName)
        {
            string[] splitName = indexName.Split (_sqlNameDivider);
            string tableName;
            switch (splitName.Length)
            {
                case 3:
                    tableName = splitName[1];
                    break;
                case 2:
                    tableName = splitName[0];
                    break;

                default:
                    throw new ArgumentException (String.Format (CultureInfo.CurrentCulture, Properties.Resources.CannotGetTableNameFromIndexName, indexName));
            }

            return _sqlNameCharactersRegex.Replace (tableName, "");
        }

        /// <summary>
        /// Format a Sql Name to escape it out properly;
        /// </summary>
        /// <param name="name">The name to escape</param>
        /// <returns>The escaped name</returns>
        internal static string FormatSqlName (string name)
        {
            return String.Format (CultureInfo.InvariantCulture, "[{0}]", UnformatSqlName (name));
        }

        /// <summary>
        /// Matches characters used to escape a sql name
        /// </summary>
        private static readonly Regex _sqlNameCharactersRegex = new Regex (@"[\[\]\""]");

        /// <summary>
        /// The divider between pieces of a sql name
        /// </summary>
        private const char _sqlNameDivider = '.';

        #endregion
	}
    #endregion
}
