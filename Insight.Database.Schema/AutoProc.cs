using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.IO;
using System.Text.RegularExpressions;
using System.Globalization;

namespace Insight.Database.Schema
{
	/// <summary>
	/// Handles creation & destruction of AutoCrud stored procedures.
	/// Allows you to simplify procedures in your scripts:
	///		AUTOPROC [type] [table] [_name]
	///		GO
	///		AUTOPROC Insert [Beer] InsertBeer
	///		GO
	/// </summary>
	class AutoProc
	{
		#region Private Members
		/// <summary>
		/// A string that is added to the hash of the dependencies so that the AutoProc can be forced to change if the internal implementation changes.
		/// </summary>
		private static string VersionSignature = "1.1.2.23";

		/// <summary>
		/// The name of the table that we are generating procedures for.
		/// </summary>
		private string _tableName;

		/// <summary>
		/// The singular form of the name of the table that we are generating procedures for.
		/// </summary>
		private string _singularTableName;

		/// <summary>
		/// The plural form of the name of the table that we are generating procedures for.
		/// </summary>
		private string _pluralTableName;

		/// <summary>
		/// The type of the procedure to generate.
		/// </summary>
		private ProcTypes _type;

		/// <summary>
		/// When set to true, dynamic SQL is executed as the owner of the procedure instead of the current user.
		/// This allows you to require all access to tables be done through the stored procedures, but still allow access
		/// to the Find procedure and other dynamic procedures.
		/// </summary>
		private bool _executeAsOwner;

		/// <summary>
		/// Provides the list of columns for a table.
		/// </summary>
		private IColumnDefinitionProvider _columnProvider;

		/// <summary>
		/// The RegEx used to detect and decode an AutoProc.
		/// </summary>
		internal static readonly string AutoProcRegex = String.Format(CultureInfo.InvariantCulture, @"AUTOPROC\s+(?<type>\w+)\s+(?<tablename>{0})(\s+Single=(?<single>[^\s]+))?(\s+Name=(?<name>[^\s]+))?(\s+ExecuteAsOwner=(?<execasowner>[^\s]+))?", SchemaObject.SqlNameExpression);

		/// <summary>
		/// The signature of the AutoProc. This is derived from the table and the private key(s) in the script collection.
		/// </summary>
		internal string Signature { get; private set; }

		/// <summary>
		/// The name of the procedure to generate.
		/// </summary>
		internal string Name { get; private set; }

		/// <summary>
		/// Gets the Sql for the AutoProc. Note that the objects must exist in the database so that the ColumnProvider can read them.
		/// </summary>
		public string Sql { get { return GenerateSql(); } }
		#endregion

		#region Constructors
		/// <summary>
		/// Initializes an automatically generated procedure.
		/// </summary>
		/// <param name="installer">The installer to use when modifying the procedure.</param>
		/// <param name="name">The name of the procedure, in in the format of the AutoProcRegex.</param>
		/// <param name="objects">The list of objects that are in the schema. Used for change detection.</param>
		public AutoProc(string name, IColumnDefinitionProvider columnProvider, IEnumerable<SchemaObject> objects)
		{
			// initialize dependencies
			_columnProvider = columnProvider;

			// break up the name into its components
			var match = new Regex(AutoProcRegex, RegexOptions.IgnoreCase).Match(name);
			_type = (ProcTypes)Enum.Parse(typeof(ProcTypes), match.Groups["type"].Value);
			_tableName = SchemaObject.FormatSqlName(match.Groups["tablename"].Value);

			// generate the singular table name
			if (!String.IsNullOrWhiteSpace(match.Groups["single"].Value))
				_singularTableName = SchemaObject.FormatSqlName(match.Groups["single"].Value);
			else
				_singularTableName = SchemaObject.FormatSqlName(Singularizer.Singularize(SchemaObject.UnformatSqlName(match.Groups["tablename"].Value)));

			// generate the plural table name
			if (!String.IsNullOrWhiteSpace(match.Groups["plural"].Value))
				_pluralTableName = SchemaObject.FormatSqlName(match.Groups["plural"].Value);
			else
			{
				_pluralTableName = _tableName;
				if (String.Compare(_pluralTableName, _singularTableName, StringComparison.OrdinalIgnoreCase) == 0)
					_pluralTableName = SchemaObject.FormatSqlName(SchemaObject.UnformatSqlName(_tableName) + "s");
			}

			// get the specified name
			string procName = match.Groups["name"].Value;
			if (!String.IsNullOrWhiteSpace(procName))
				Name = SchemaObject.FormatSqlName(procName);

			//  check the exec as owner flag
			if (!String.IsNullOrWhiteSpace(match.Groups["execasowner"].Value))
				_executeAsOwner = Boolean.Parse(match.Groups["execasowner"].Value);

			// if we received a set of objects, then we can calculate a signature
			if (objects != null)
			{
				Regex optionalSqlName = new Regex(@"([\[\]])");
				string escapedWildcardedName = optionalSqlName.Replace(Regex.Escape(_tableName), @"$1?");
				Regex regex = new Regex(String.Format(CultureInfo.InvariantCulture, @"(CREATE\s+TABLE\s+{0})|(ALTER\s+TABLE\s+{0}.*PRIMARY\s+KEY)", escapedWildcardedName));

				// calculate the signature based upon the TABLE definition, plus any PRIMARY KEY definition for the table
				string sql = String.Join(" ", objects.Where(o => regex.Match(o.Sql).Success).Select(o => o.Sql));

				// add a version signature so we can force updates if we need to
				sql += VersionSignature;

				Signature = SchemaObject.CalculateSignature(sql);
			}
			else
			{
				// we don't know what the schema is, so assume that the proc has changed
				Signature = Guid.NewGuid().ToString();
			}
		}
		#endregion

		#region Install Methods
		/// <summary>
		/// Generate the Sql required for this procedure.
		/// </summary>
		/// <returns>The sql for this procedure.</returns>
		private string GenerateSql()
		{
			IList<ColumnDefinition> columns = _columnProvider.GetColumns(_tableName);

			// generate the sql
			string sql = "";

			if (_type.HasFlag(ProcTypes.Table)) sql += GenerateTableSql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.IdTable)) sql += GenerateIdTableSql(columns) + " GO" + Environment.NewLine;

			if (_type.HasFlag(ProcTypes.Select)) sql += GenerateSelectSql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.Insert)) sql += GenerateInsertSql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.Update)) sql += GenerateUpdateSql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.Upsert)) sql += GenerateUpsertSql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.Delete)) sql += GenerateDeleteSql(columns) + " GO" + Environment.NewLine;

			if (_type.HasFlag(ProcTypes.SelectMany)) sql += GenerateSelectManySql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.InsertMany)) sql += GenerateInsertManySql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.UpdateMany)) sql += GenerateUpdateManySql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.UpsertMany)) sql += GenerateUpsertManySql(columns) + " GO" + Environment.NewLine;
			if (_type.HasFlag(ProcTypes.DeleteMany)) sql += GenerateDeleteManySql(columns) + " GO" + Environment.NewLine;

			if (_type.HasFlag(ProcTypes.Find)) sql += GenerateFindSql(columns) + " GO" + Environment.NewLine;

			return sql;
		}
		#endregion

		#region Standard CRUD Sql
		/// <summary>
		/// Generates the Select procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateSelectSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0}", MakeProcName("Select", plural: false));
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, ",", "{1} {2}"));
			sb.AppendLine(")");
			sb.AppendLine("AS");
			sb.AppendFormat("SELECT * FROM {0} WHERE ", _tableName);
			sb.AppendLine();
			sb.AppendLine(Join(keys, " AND", "{0}={1}"));

			return sb.ToString();
		}

		/// <summary>
		/// Generates the Insert procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateInsertSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> outputs = columns.Where(c => c.IsReadOnly);
			IEnumerable<ColumnDefinition> insertable = columns.Where(c => !c.IsReadOnly);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0}", MakeProcName("Insert", plural: false));
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(insertable, ",", "{1} {2}"));
			sb.AppendLine(")");
			sb.AppendLine("AS");
			sb.AppendFormat("INSERT INTO {0}", _tableName);
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(insertable, ",", "{0}"));
			sb.AppendLine(")");
			if (outputs.Any())
			{
				sb.AppendLine("OUTPUT");
				sb.AppendLine(Join(outputs, ",", "Inserted.{0}"));
			}
			sb.AppendLine("VALUES");
			sb.AppendLine("(");
			sb.AppendLine(Join(insertable, ",", "{1}"));
			sb.AppendLine(")");

			return sb.ToString();
		}

		/// <summary>
		/// Generates the Update procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateUpdateSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> inputs = columns.Where(c => c.IsKey || !c.IsReadOnly);
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);
			IEnumerable<ColumnDefinition> updatable = columns.Where(c => !c.IsKey && !c.IsReadOnly);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0}", MakeProcName("Update", plural: false));
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(inputs, ",", "{1} {2}"));
			sb.AppendLine(")");
			sb.AppendLine("AS");

			if (updatable.Any())
			{
				sb.AppendFormat("UPDATE {0} SET", _tableName);
				sb.AppendLine();
				sb.AppendLine(Join(updatable, ",", "{0}={1}"));
				sb.AppendLine("WHERE");
				sb.AppendLine(Join(keys, " AND", "{0}={1}"));
			}
			else
			{
				sb.AppendFormat("RAISERROR (N'There are no UPDATEable fields on {0}', 18, 0)", _tableName);
				sb.AppendLine();
			}

			return sb.ToString();
		}

		/// <summary>
		/// Generates the Upsert procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateUpsertSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> inputs = columns.Where(c => c.IsKey || !c.IsReadOnly);
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);
			IEnumerable<ColumnDefinition> updatable = columns.Where(c => !c.IsKey && !c.IsReadOnly);
			IEnumerable<ColumnDefinition> outputs = columns.Where(c => c.IsReadOnly);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0}", MakeProcName("Upsert", plural: false));
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(inputs, ",", "{1} {2}"));
			sb.AppendLine(")");
			sb.AppendLine("AS");

			if (updatable.Any())
			{
				sb.AppendFormat("MERGE INTO {0} AS t", _tableName);
				sb.AppendLine();
				sb.AppendLine("USING");
				sb.AppendLine("(");
				sb.AppendLine("SELECT");
				sb.AppendLine(Join(inputs, ",", "{0} = {1}"));
				sb.AppendLine(")");
				sb.AppendLine("AS s");
				sb.AppendLine("ON");
				sb.AppendLine("(");
				sb.AppendLine(Join(keys, " AND", "t.{0} = s.{0}"));
				sb.AppendLine(")");
				sb.AppendLine("WHEN MATCHED THEN UPDATE SET");
				sb.AppendLine(Join(updatable, ",", "\tt.{0} = s.{0}"));
				sb.AppendLine("WHEN NOT MATCHED BY TARGET THEN INSERT");
				sb.AppendLine("(");
				sb.AppendLine(Join(updatable, ",", "{0}"));
				sb.AppendLine(")");
				sb.AppendLine("VALUES");
				sb.AppendLine("(");
				sb.AppendLine(Join(updatable, ",", "s.{0}"));
				sb.AppendLine(")");
				sb.AppendLine("OUTPUT");
				sb.AppendLine(Join(outputs, ",", "Inserted.{0}"));
				sb.AppendLine(";");
			}
			else
			{
				sb.AppendFormat("RAISERROR (N'There are no UPDATEable fields on {0}', 18, 0)", _tableName);
				sb.AppendLine();
			}

			return sb.ToString();
		}

		/// <summary>
		/// Generates the Delete procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateDeleteSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0}", MakeProcName("Delete", plural: false));
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, ",", "{1} {2}"));
			sb.AppendLine(")");
			sb.AppendLine("AS");
			sb.AppendFormat("DELETE FROM {0} WHERE", _tableName);
			sb.AppendLine();
			sb.AppendLine(Join(keys, " AND", "{0}={1}"));

			return sb.ToString();
		}
		#endregion

		#region Multiple CRUD Sql
		/// <summary>
		/// Generates the Table Type.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateTableSql(IList<ColumnDefinition> columns)
		{
			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE TYPE {0}", MakeTableName("Table"));
			sb.AppendLine();
			sb.AppendLine("AS TABLE");
			sb.AppendLine("(");
			sb.AppendLine(Join(columns, ",", "{0} {2}"));
			sb.AppendLine(")");

			return sb.ToString();
		}

		/// <summary>
		/// Generates the ID Table type.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateIdTableSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE TYPE {0}", MakeTableName("IdTable"));
			sb.AppendLine();
			sb.AppendLine("AS TABLE");
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, ",", "{0} {2}"));
			sb.AppendLine(")");

			return sb.ToString();
		}

		/// <summary>
		/// Generates the SelectMany procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateSelectManySql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			string parameterName = SchemaObject.UnformatSqlName(_singularTableName);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (@{1} {2} READONLY)", MakeProcName("Select", plural: true), parameterName, MakeTableName("IdTable"));
			sb.AppendLine();
			sb.AppendLine("AS");
			sb.AppendFormat("SELECT * FROM {0} AS t", _tableName);
			sb.AppendLine();
			sb.AppendFormat("JOIN @{0} AS s ON", parameterName);
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, " AND", "t.{0} = s.{0}"));
			sb.AppendLine(")");

			return sb.ToString();
		}

		/// <summary>
		/// Generates the InsertMany procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateInsertManySql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> outputs = columns.Where(c => c.IsReadOnly);
			IEnumerable<ColumnDefinition> insertable = columns.Where(c => !c.IsReadOnly);

			string parameterName = SchemaObject.UnformatSqlName(_singularTableName);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (@{1} {2} READONLY)", MakeProcName("Insert", plural: true), parameterName, MakeTableName("Table"));
			sb.AppendLine();
			sb.AppendLine("AS");
			sb.AppendFormat("INSERT INTO {0}", _tableName);
			sb.AppendLine();
			if (insertable.Any())
			{
				sb.AppendLine("(");
				sb.AppendLine(Join(insertable, ",", "{0}"));
				sb.AppendLine(")");
			}
			if (outputs.Any())
			{
				sb.AppendLine("OUTPUT");
				sb.AppendLine(Join(outputs, ",", "Inserted.{0}"));
			}
			sb.AppendLine("SELECT");
			sb.AppendLine(Join(insertable, ",", "{0}"));
			sb.AppendFormat("FROM @{1}", _tableName, parameterName);
			sb.AppendLine();

			return sb.ToString();
		}

		/// <summary>
		/// Generates the UpdateMany procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateUpdateManySql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);
			IEnumerable<ColumnDefinition> updatable = columns.Where(c => !c.IsReadOnly);

			string parameterName = SchemaObject.UnformatSqlName(_singularTableName);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (@{1} {2} READONLY)", MakeProcName("Update", plural: true), parameterName, MakeTableName("Table"));
			sb.AppendLine();
			sb.AppendLine("AS");
			sb.AppendFormat("MERGE INTO {0} AS t", _tableName);
			sb.AppendLine();
			sb.AppendFormat("USING @{0} AS s", parameterName);
			sb.AppendLine();
			sb.AppendLine("ON");
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, " AND", "t.{0} = s.{0}"));
			sb.AppendLine(")");
			sb.AppendLine("WHEN MATCHED THEN UPDATE SET");
			sb.AppendLine(Join(updatable, ",", "t.{0} = s.{0}"));
			sb.AppendLine(";");
			return sb.ToString();
		}

		/// <summary>
		/// Generates the UpsertMany procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateUpsertManySql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);
			IEnumerable<ColumnDefinition> outputs = columns.Where(c => c.IsReadOnly);
			IEnumerable<ColumnDefinition> updatable = columns.Where(c => !c.IsReadOnly);

			string parameterName = SchemaObject.UnformatSqlName(_singularTableName);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (@{1} {2} READONLY)", MakeProcName("Upsert", plural: true), parameterName, MakeTableName("Table"));
			sb.AppendLine();
			sb.AppendLine("AS");
			sb.AppendFormat("MERGE INTO {0} AS t", _tableName);
			sb.AppendLine();
			sb.AppendFormat("USING @{0} AS s", parameterName);
			sb.AppendLine();
			sb.AppendLine("ON");
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, " AND", "t.{0} = s.{0}"));
			sb.AppendLine(")");
			sb.AppendLine("WHEN MATCHED THEN UPDATE SET");
			sb.AppendLine(Join(updatable, ",", "t.{0} = s.{0}"));
			sb.AppendLine("WHEN NOT MATCHED BY TARGET THEN INSERT");
			sb.AppendLine("(");
			sb.AppendLine(Join(updatable, ",", "{0}"));
			sb.AppendLine(")");
			sb.AppendLine("VALUES");
			sb.AppendLine("(");
			sb.AppendLine(Join(updatable, ",", "s.{0}"));
			sb.AppendLine(")");
			if (outputs.Any())
			{
				sb.AppendLine("OUTPUT");
				sb.AppendLine(Join(outputs, ",", "Inserted.{0}"));
			}
			sb.AppendLine(";");

			return sb.ToString();
		}

		/// <summary>
		/// Generates the DeleteMany procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateDeleteManySql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			string parameterName = SchemaObject.UnformatSqlName(_singularTableName);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (@{1} {2} READONLY)", MakeProcName("Delete", plural: true), parameterName, MakeTableName("IdTable"));
			sb.AppendLine();
			sb.AppendLine("AS");
			sb.AppendFormat("DELETE FROM {0}", _tableName);
			sb.AppendLine();
			sb.AppendFormat("\tFROM {0} AS t", _tableName);
			sb.AppendLine();
			sb.AppendFormat("JOIN @{0} AS s ON", parameterName);
			sb.AppendLine();
			sb.AppendLine("(");
			sb.AppendLine(Join(keys, " AND", "t.{0} = s.{0}"));
			sb.AppendLine(")");

			return sb.ToString();
		}
		#endregion

		#region Find Sql
		/// <summary>
		/// Generates the Find procedure.
		/// </summary>
		/// <param name="columns">The list of columns in the table.</param>
		/// <returns>The stored procedure SQL.</returns>
		private string GenerateFindSql(IList<ColumnDefinition> columns)
		{
			string parameterName = SchemaObject.UnformatSqlName(_singularTableName);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0}", MakeProcName("Find", plural: true));
			sb.AppendLine();
			sb.AppendLine("(");
			sb.Append(Join(columns, ",", "{1} {2} = NULL"));
			sb.AppendLine(",");
			sb.AppendLine(Join(columns, ",", "{1}Operator [varchar](5) = '='"));
			sb.AppendLine(")");
			if(_executeAsOwner)
				sb.AppendLine("WITH EXECUTE AS OWNER");
			sb.AppendLine("AS");
			sb.AppendFormat("DECLARE @sql [nvarchar](MAX) = 'SELECT * FROM {0} WHERE 1=1'", _tableName);
			sb.AppendLine();
			sb.AppendLine(Join(columns, "", "IF {1} IS NOT NULL SELECT @sql = @sql + ' AND {1} ' + {1}Operator + ' {0}'"));
			sb.AppendLine("EXEC sp_executesql @sql, N'");
			sb.AppendLine(Join(columns, ",", "{1} {2}"));
			sb.AppendLine("',");
			sb.AppendLine(Join(columns, ",", "{1}={1}"));

			return sb.ToString();
		}
		#endregion

		#region Proc Name Methods
		/// <summary>
		/// Make a procedure name for a given type of procedure.
		/// </summary>
		/// <param name="type">The type of procedure to make.</param>
		/// <returns>The name of the procedure.</returns>
		private string MakeProcName(string type, bool plural)
		{
			// use the user-specified name or make one from the type
			return SchemaObject.FormatSqlName(String.Format (CultureInfo.InvariantCulture, Name ?? (plural ? "{0}{1}" : "{0}{2}"), 
				type,
				_pluralTableName,
				_singularTableName));
		}

		/// <summary>
		/// Make a type type name.
		/// </summary>
		/// <param name="type">The type of table to make.</param>
		/// <returns>The name of the table.</returns>
		private string MakeTableName(string type)
		{
			// use the user-specified name or make one from the type
			return SchemaObject.FormatSqlName(String.Format(CultureInfo.InvariantCulture, Name ?? "{2}{0}",
				type,
				_tableName,
				_singularTableName));
		}

		/// <summary>
		/// Make a drop statement for a given type of procedure.
		/// </summary>
		/// <param name="type">The type of procedure to make.</param>
		/// <param name="plural">True to generate the name with a plural number of objects.</param>
		/// <returns>The Drop statement.</returns>
		private string MakeDropStatment(string type, bool plural)
		{
			return String.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{0}') AND type in (N'P', N'PC')) DROP PROCEDURE {0}", MakeProcName(type, plural));
		}

		/// <summary>
		/// Make a drop statement for a table type.
		/// </summary>
		/// <param name="tableName">The name of the table to drop.</param>
		/// <returns>The Drop statement.</returns>
		private string MakeTableDropStatment(string tableName)
		{
			return String.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'{1}') DROP TYPE {0}", MakeTableName(tableName), SchemaObject.UnformatSqlName(MakeTableName(tableName)));
		}
		#endregion

		#region Drop Methods
		/// <summary>
		/// Return the Sql to drop the autoproc.
		/// </summary>
		public string DropSql
		{
			get
			{
				string sql = "";
				if (_type.HasFlag(ProcTypes.Select)) sql += MakeDropStatment("Select", plural: false) + " GO ";
				if (_type.HasFlag(ProcTypes.Insert)) sql += MakeDropStatment("Insert", plural: false) + " GO ";
				if (_type.HasFlag(ProcTypes.Update)) sql += MakeDropStatment("Update", plural: false) + " GO ";
				if (_type.HasFlag(ProcTypes.Upsert)) sql += MakeDropStatment("Upsert", plural: false) + " GO ";
				if (_type.HasFlag(ProcTypes.Delete)) sql += MakeDropStatment("Delete", plural: false) + " GO ";
				if (_type.HasFlag(ProcTypes.SelectMany)) sql += MakeDropStatment("Select", plural: true) + " GO ";
				if (_type.HasFlag(ProcTypes.InsertMany)) sql += MakeDropStatment("Insert", plural: true) + " GO ";
				if (_type.HasFlag(ProcTypes.UpdateMany)) sql += MakeDropStatment("Update", plural: true) + " GO ";
				if (_type.HasFlag(ProcTypes.UpsertMany)) sql += MakeDropStatment("Upsert", plural: true) + " GO ";
				if (_type.HasFlag(ProcTypes.DeleteMany)) sql += MakeDropStatment("Delete", plural: true) + " GO ";
				if (_type.HasFlag(ProcTypes.Find)) sql += MakeDropStatment("Find", plural: true) + " GO ";

				if (_type.HasFlag(ProcTypes.Table)) sql += MakeTableDropStatment("Table") + " GO ";
				if (_type.HasFlag(ProcTypes.IdTable)) sql += MakeTableDropStatment("IdTable") + " GO ";

				return sql;
			}
		}
		#endregion

		#region Helper Functions
		/// <summary>
		/// Creates a column list with the specified divider and template.
		/// </summary>
		/// <param name="columns">The columns to emit.</param>
		/// <param name="divider">The divider to use for the columns.</param>
		/// <param name="template">The template to use for each column.</param>
		/// <returns>The delimited list of column names.</returns>
		private static string Join(IEnumerable<ColumnDefinition> columns, string divider, string template)
		{
			return String.Join(divider + Environment.NewLine, columns.Select(col => String.Format(CultureInfo.InvariantCulture, "\t" + template, col.ColumnName, col.ParameterName, col.SqlType)));
		}
		#endregion

		#region Private Classes
		/// <summary>
		/// The types of stored procedures that we support.
		/// </summary>
		[Flags]
		enum ProcTypes
		{
			Table = 1 << 0,
			IdTable = 1 << 1,
			Select = 1 << 2,
			Insert = 1 << 3,
			Update = 1 << 4,
			Upsert = 1 << 5,
			Delete = 1 << 6,
			SelectMany = 1 << 7,
			InsertMany = 1 << 8,
			UpdateMany = 1 << 9,
			UpsertMany = 1 << 10,
			DeleteMany = 1 << 11,
			Find = 1 << 12,

			All = 
				Table | IdTable | 
				Select | Insert | Update | Upsert | Delete | 
				SelectMany | InsertMany | UpdateMany | UpsertMany | DeleteMany |
				Find
		}
		#endregion
	}
}
