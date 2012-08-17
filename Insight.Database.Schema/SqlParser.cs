using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Insight.Database.Schema.Properties;

namespace Insight.Database.Schema
{
	#region SqlParserClass
	/// <summary>
	/// A parser that detects the type and name of a sql object from a script.
	/// </summary>
	internal class SqlParser
	{
		#region List of Parser Templates
		/// <summary>
		/// Initializes the list of SQL parsers.
		/// </summary>
		static SqlParser()
		{
			List<SqlParser> parsers = new List<SqlParser>();

			parsers.Add(new SqlParser(SchemaObjectType.IndexedView, String.Format(CultureInfo.InvariantCulture, @"--\s*INDEXEDVIEW.+CREATE\s+VIEW\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.UserPreScript, String.Format(CultureInfo.InvariantCulture, @"--\s*PRESCRIPT\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.UserScript, String.Format(CultureInfo.InvariantCulture, @"--\s*SCRIPT\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.UserDefinedType, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+TYPE\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.UserDefinedType, String.Format(CultureInfo.InvariantCulture, @"EXEC(UTE)?\s+sp_addtype\s+'?(?<name>{0})'?", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.MasterKey, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+MASTER\s+KEY\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.Certificate, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+CERTIFICATE\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.SymmetricKey, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+SYMMETRIC\s+KEY\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.PartitionFunction, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+PARTITION\s+FUNCTION\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.PartitionScheme, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+PARTITION\s+SCHEME\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.MessageType, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+MESSAGE TYPE\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.Contract, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+CONTRACT\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.BrokerPriority, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+BROKER\s+PRIORITY\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.Queue, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+QUEUE\s+(?<name>{0})", SqlNameExpression), "QUEUE $1"));
			parsers.Add(new SqlParser(SchemaObjectType.Service, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+SERVICE\s+(?<name>{0})", SqlNameExpression), "SERVICE $1"));
			parsers.Add(new SqlParser(SchemaObjectType.Table, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+TABLE\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.Trigger, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+TRIGGER\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.Index, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+(UNIQUE\s+)?(((CLUSTERED)|(NONCLUSTERED))\s+)?INDEX\s+(?<indname>{0})\s+ON\s+(?<tablename>{0})", SqlNameExpression), "$2.$1"));
			parsers.Add(new SqlParser(SchemaObjectType.View, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+VIEW\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.StoredProcedure, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+PROC(EDURE)?\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.Permission, String.Format(CultureInfo.InvariantCulture, @"GRANT\s+(?<permission>{0})\s+ON\s+(?<name>{0})\s+TO\s+(?<grantee>{0})", SqlNameExpression), "$1 ON $2 TO $3"));
			parsers.Add(new SqlParser(SchemaObjectType.PrimaryKey, String.Format(CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+(?<tablename>{0})\s+(WITH\s+(NO)?CHECK\s+)?ADD\s+CONSTRAINT\s*\(?(?<name>{0})\)?\s+PRIMARY\s+", SqlNameExpression), "$1.$2"));
			parsers.Add(new SqlParser(SchemaObjectType.ForeignKey, String.Format(CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+(?<tablename>{0})\s+(WITH\s+(NO)?CHECK\s+)?ADD\s+CONSTRAINT\s*\(?(?<name>{0})\)?\s+FOREIGN\s+KEY\s*\(?(?<name>{0})\)?", SqlNameExpression), "$1.$2"));
			parsers.Add(new SqlParser(SchemaObjectType.Constraint, String.Format(CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+(?<tablename>{0})\s+(WITH\s+(NO)?CHECK\s+)?ADD\s+CONSTRAINT\s*\(?(?<name>{0})\)?", SqlNameExpression), "$1.$2"));
			parsers.Add(new SqlParser(SchemaObjectType.Function, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+FUNCTION\s+(?<name>{0})", SqlNameExpression)));
			parsers.Add(new SqlParser(SchemaObjectType.PrimaryXmlIndex, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+PRIMARY\s+XML\s+INDEX\s+(?<name>{0})\s+ON\s+(?<tablename>{0})", SqlNameExpression), "$2.$1"));
			parsers.Add(new SqlParser(SchemaObjectType.SecondaryXmlIndex, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+XML\s+INDEX\s+(?<name>{0})\s+ON\s+(?<tablename>{0})", SqlNameExpression), "$2.$1"));
			parsers.Add(new SqlParser(SchemaObjectType.Login, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+LOGIN\s+(?<name>{0})", SqlNameExpression), "LOGIN $1"));
			parsers.Add(new SqlParser(SchemaObjectType.User, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+USER\s+(?<name>{0})", SqlNameExpression), "USER $1"));
			parsers.Add(new SqlParser(SchemaObjectType.Role, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+ROLE\s+(?<name>{0})", SqlNameExpression), "ROLE $1"));
			parsers.Add(new SqlParser(SchemaObjectType.Schema, String.Format(CultureInfo.InvariantCulture, @"CREATE\s+SCHEMA\s+(?<name>{0})", SqlNameExpression), "SCHEMA $1"));
			parsers.Add(new SqlParser(SchemaObjectType.Unused, String.Format(CultureInfo.InvariantCulture, @"SET\s+ANSI_NULLS", SqlNameExpression), null));
			parsers.Add(new SqlParser(SchemaObjectType.Unused, String.Format(CultureInfo.InvariantCulture, @"SET\s+QUOTED_IDENTIFIER", SqlNameExpression), null));
			parsers.Add(new SqlParser(SchemaObjectType.AutoProc, AutoProc.AutoProcRegex, "$0"));

			// make sure that they are sorted in the order of likelihood
			parsers.Sort((p1, p2) => p1.SchemaObjectType.CompareTo(p2.SchemaObjectType));

			Parsers = new ReadOnlyCollection<SqlParser>(parsers); 
		}

		internal static readonly ReadOnlyCollection<SqlParser> Parsers;

		/// <summary>
		/// Matches a SQL name in the form [a].[b].[c], or "a"."b"."c" or a.b.c (or any combination)
		/// Also: TYPE :: sqlname for global scoping
		/// </summary>
		internal const string SqlNameExpression = @"([\w\d]+\s*::\s*)?((\[[^\]]+\]|[\w\d]+)\.){0,2}((\[[^\]]+\]|[\w\d]+))";
		#endregion

		#region Constructors
		/// <summary>
		/// Create a parser that detects a type from a pattern
		/// </summary>
		/// <param name="type">The type represented by the pattern</param>
		/// <param name="pattern">The pattern to detect</param>
		/// <param name="nameTemplate">The string used to generate the resulting name</param>
		public SqlParser(SchemaObjectType type, string pattern, string nameTemplate = "$1")
		{
			SchemaObjectType = type;
			_regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.ExplicitCapture | RegexOptions.Compiled);
			_nameTemplate = nameTemplate;
		}
		#endregion

		internal class SqlParserMatch
		{
			public SchemaObjectType SchemaObjectType;
			public string Name;
			public int Position;
		}

		#region Match Methods
		/// <summary>
		/// Attempts to match the sql to the pattern. If successful, sets the type and name
		/// </summary>
		/// <param name="sql">The sql to parse</param>
		/// <param name="type">If matched, updated to the current type</param>
		/// <param name="name">If matched, updated to the name</param>
		/// <returns></returns>
		public SqlParserMatch Match(string sql)
		{
			Match match = _regex.Match(sql);
			if (match.Success)
			{
				// convert the name to the templated name
				string name = match.Result(_nameTemplate);
				string[] pieces;

				switch (SchemaObjectType)
				{
					case SchemaObjectType.Service:
					case SchemaObjectType.Queue:
					case SchemaObjectType.Role:
						pieces = name.Split(new char[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);
						name = pieces[0] + " " + FormatSqlName(pieces[1]);
						break;

					case SchemaObjectType.Permission:
					case SchemaObjectType.AutoProc:
						break;

					default:
						// for most types, reformat the name to a fully qualified sql name
						pieces = name.Split(_sqlNameDivider);
						name = "";
						for (int i = pieces.Length - _nameTemplate.Split(_sqlNameDivider).Length; i < pieces.Length; i++)
						{
							if (name.Length > 0)
								name += ".";
							name += FormatSqlName(pieces[i]);
						}
						break;
				}

				// return a match
				return new SqlParserMatch()
				{
					SchemaObjectType = this.SchemaObjectType,
					Name = name,
					Position = match.Index
				};
			}

			return null;
		}
		#endregion

		#region Utility Formatting Methods
		/// <summary>
		/// Get the name of a SqlObject without owner and schema, and unformat the name
		/// </summary>
		/// <remarks>[dbo]..[foo] returns foo</remarks>
		/// <param name="name">The full name to clean up</param>
		/// <returns>The unformatted object name</returns>
		internal static string UnformatSqlName(string name)
		{
			string[] splitName = name.Split(_sqlNameDivider);
			string objectName = splitName[splitName.Length - 1];
			return _sqlNameCharactersRegex.Replace(objectName, "");
		}

		/// <summary>
		/// Get the table name from the name of an index
		/// </summary>
		/// <param name="indexName">The name of the index</param>
		/// <returns>The name of the table</returns>
		/// <exception cref="ArgumentException">If the table name cannot be determined</exception>
		internal static string TableNameFromIndexName(string indexName)
		{
			string[] splitName = indexName.Split(_sqlNameDivider);
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
					throw new ArgumentException(String.Format(CultureInfo.CurrentCulture, Properties.Resources.CannotGetTableNameFromIndexName, indexName));
			}

			return _sqlNameCharactersRegex.Replace(tableName, "");
		}

		/// <summary>
		/// Get the index name [IX_foo] from the full name of an index [foo].[ix_foo]
		/// </summary>
		/// <param name="indexName">The name of the index</param>
		/// <returns>The name of the table</returns>
		/// <exception cref="ArgumentException">If the table name cannot be determined</exception>
		internal static string IndexNameFromFullName(string indexName)
		{
			return UnformatSqlName(indexName.Split(_sqlNameDivider).Last());
		}

		/// <summary>
		/// Format a Sql Name to escape it out properly;
		/// </summary>
		/// <param name="name">The name to escape</param>
		/// <returns>The escaped name</returns>
		internal static string FormatSqlName(string name)
		{
			return String.Format(CultureInfo.InvariantCulture, "[{0}]", UnformatSqlName(name));
		}

		/// <summary>
		/// The divider between pieces of a sql name
		/// </summary>
		private const char _sqlNameDivider = '.';

		/// <summary>
		/// Matches characters used to escape a sql name
		/// </summary>
		private static readonly Regex _sqlNameCharactersRegex = new Regex(@"[\[\]\""]");

		/// <summary>
		/// Makes sure that a name of a schema object does not contain any insecure characters.
		/// </summary>
		/// <param name="name">The name to check</param>
		/// <exception cref="ArgumentException">If a parameter contains an invalid SQL character</exception>
		/// <exception cref="ArgumentNullException">If the name is null</exception>
		internal static void AssertValidSqlName(string name)
		{
			if (name == null)
				throw new ArgumentNullException("name");
			if (name.Length == 0 || name.IndexOfAny(_insecureSqlChars) >= 0)
				throw new ArgumentException(String.Format(CultureInfo.CurrentCulture, Resources.InvalidSqlObjectName, name));
		}

		/// <summary>
		/// Characters that could cause bad sql things to happen
		/// </summary>
		/// <remarks>
		///     -   can create a comment
		///     ;   can end a statement
		///     '   can end a string
		///     "   can end a string
		/// </remarks>
		private static readonly char[] _insecureSqlChars = new char[] { '-', ';', '\'' };
		#endregion


		#region Private Members
		/// <summary>
		/// The expression pattern to match
		/// </summary>
		private Regex _regex;

		/// <summary>
		/// The corresponding object type
		/// </summary>
		public SchemaObjectType SchemaObjectType { get; private set; }

		/// <summary>
		/// The string used to generate the name from the match
		/// </summary>
		private string _nameTemplate;
		#endregion
	}
	#endregion
}
