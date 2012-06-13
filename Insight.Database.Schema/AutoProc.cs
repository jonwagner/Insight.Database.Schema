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
		/// The signature of the AutoProc. This is derived from the table and the private key(s) in the script collection.
		/// </summary>
		internal string Signature { get; private set; }

		/// <summary>
		/// The name of the table that we are generating procedures for.
		/// </summary>
		private string _tableName;

		/// <summary>
		/// The name of the procedure to generate.
		/// </summary>
		internal string Name { get; private set; }

		/// <summary>
		/// The type of the procedure to generate.
		/// </summary>
		private ProcTypes _type;

		/// <summary>
		/// Provides the list of columns for a table.
		/// </summary>
		private IColumnDefinitionProvider _columnProvider;

		/// <summary>
		/// The RegEx used to detect and decode an AutoProc.
		/// </summary>
		internal static readonly string AutoProcRegex = String.Format(CultureInfo.InvariantCulture, @"AUTOPROC\s+(?<type>\w+)\s+(?<tablename>{0})(\s+(?<name>[^\s]+))?", SchemaObject.SqlNameExpression);

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
			var match = new Regex(AutoProcRegex).Match(name);
			_type = (ProcTypes)Enum.Parse(typeof(ProcTypes), match.Groups["type"].Value);
			_tableName = SchemaObject.FormatSqlName(match.Groups["tablename"].Value);

			// get the specified name
			string procName = match.Groups["name"].Value;
			if (!String.IsNullOrWhiteSpace(procName))
				Name = SchemaObject.FormatSqlName(procName);

			// if we received a set of objects, then we can calculate a signature
			if (objects != null)
			{
				Regex optionalSqlName = new Regex(@"([\[\]])");
				string escapedWildcardedName = optionalSqlName.Replace(Regex.Escape(_tableName), @"$1?");
				Regex regex = new Regex(String.Format(CultureInfo.InvariantCulture, @"(CREATE\s+TABLE\s+{0})|(ALTER\s+TABLE\s+{0}.*PRIMARY\s+KEY)", escapedWildcardedName));

				// calculate the signature based upon the TABLE definition, plus any PRIMARY KEY definition for the table
				string sql = String.Join(" ", objects.Where(o => regex.Match(o.Sql).Success).Select(o => o.Sql));
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

			if (_type.HasFlag(ProcTypes.Select)) sql += GenerateSelectSql(columns) + " GO ";
			if (_type.HasFlag(ProcTypes.Insert)) sql += GenerateInsertSql(columns) + " GO ";
			if (_type.HasFlag(ProcTypes.Update)) sql += GenerateUpdateSql(columns) + " GO ";
			if (_type.HasFlag(ProcTypes.Delete)) sql += GenerateDeleteSql(columns) + " GO ";

			return sql;
		}

		private string GenerateSelectSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (", MakeProcName("Select"));
			sb.Append(String.Join(", ", keys.Select(col => String.Format(CultureInfo.InvariantCulture, "@{0} {1}", col.Name, col.SqlType))));
			sb.AppendFormat(") AS SELECT * FROM {0} WHERE ", _tableName);
			sb.Append(String.Join(" AND ", keys.Select(col => String.Format(CultureInfo.InvariantCulture, "{0}=@{0}", col.Name))));

			return sb.ToString();
		}

		private string GenerateInsertSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> identities = columns.Where(c => c.IsReadOnly);
			IEnumerable<ColumnDefinition> insertable = columns.Where(c => !c.IsReadOnly);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (", MakeProcName("Insert"));
			sb.Append(String.Join(", ", insertable.Select(col => String.Format(CultureInfo.InvariantCulture, "@{0} {1}", col.Name, col.SqlType))));
			sb.AppendFormat(") AS INSERT INTO {0} (", _tableName);
			sb.Append(String.Join(", ", insertable.Select(col => String.Format(CultureInfo.InvariantCulture, "{0}", col.Name))));
			sb.Append(") OUTPUT ");
			sb.Append(String.Join(", ", identities.Select(col => String.Format(CultureInfo.InvariantCulture, "Inserted.{0}", col.Name))));
			sb.Append(" VALUES (");
			sb.Append(String.Join(", ", insertable.Select(col => String.Format(CultureInfo.InvariantCulture, "@{0}", col.Name))));
			sb.Append(")");

			return sb.ToString();
		}

		private string GenerateUpdateSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> inputs = columns.Where(c => c.IsKey || !c.IsReadOnly);
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);
			IEnumerable<ColumnDefinition> updatable = columns.Where(c => !c.IsKey && !c.IsReadOnly);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (", MakeProcName("Update"));
			sb.Append(String.Join(", ", inputs.Select(col => String.Format(CultureInfo.InvariantCulture, "@{0} {1}", col.Name, col.SqlType))));
			sb.AppendFormat(") AS UPDATE {0} SET ", _tableName);
			sb.Append(String.Join(", ", updatable.Select(col => String.Format(CultureInfo.InvariantCulture, "{0}=@{0}", col.Name))));
			sb.Append(" WHERE ");
			sb.Append(String.Join(" AND ", keys.Select(col => String.Format(CultureInfo.InvariantCulture, "{0}=@{0}", col.Name))));

			return sb.ToString();
		}

		private string GenerateDeleteSql(IList<ColumnDefinition> columns)
		{
			IEnumerable<ColumnDefinition> keys = columns.Where(c => c.IsKey);

			// generate the sql for each proc and install them
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("CREATE PROCEDURE {0} (", MakeProcName("Delete"));
			sb.Append(String.Join(", ", keys.Select(col => String.Format(CultureInfo.InvariantCulture, "@{0} {1}", col.Name, col.SqlType))));
			sb.AppendFormat(") AS DELETE FROM {0} WHERE ", _tableName);
			sb.Append(String.Join(" AND ", keys.Select(col => String.Format(CultureInfo.InvariantCulture, "{0}=@{0}", col.Name))));

			return sb.ToString();
		}

		/// <summary>
		/// Make a procedure name for a given type of procedure.
		/// </summary>
		/// <param name="type">The type of procedure to make.</param>
		/// <returns>The name of the procedure.</returns>
		private string MakeProcName(string type)
		{
			// use the user-specified name or make one from the type
			return SchemaObject.FormatSqlName(String.Format (CultureInfo.InvariantCulture, Name ?? "{0}{2}", 
				type,
				SchemaObject.UnformatSqlName(_tableName),
				Singularizer.Singularize(SchemaObject.UnformatSqlName(_tableName))));
		}

		/// <summary>
		/// Make a drop statement for a given type of procedure.
		/// </summary>
		/// <param name="type">The type of procedure to make.</param>
		/// <returns>The Drop statement.</returns>
		private string MakeDropStatment(string type)
		{
			return String.Format(CultureInfo.InvariantCulture, "DROP PROCEDURE {0}", MakeProcName(type));
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
				if (_type.HasFlag(ProcTypes.Select)) sql += MakeDropStatment("Select") + " GO ";
				if (_type.HasFlag(ProcTypes.Insert)) sql += MakeDropStatment("Insert") + " GO ";
				if (_type.HasFlag(ProcTypes.Update)) sql += MakeDropStatment("Update") + " GO ";
				if (_type.HasFlag(ProcTypes.Delete)) sql += MakeDropStatment("Delete") + " GO ";

				return sql;
			}
		}
		#endregion

		#region Private Classes
		/// <summary>
		/// The types of stored procedures that we support.
		/// </summary>
		[Flags]
		enum ProcTypes
		{
			Select = 1 << 0,
			Insert = 1 << 1,
			Update = 1 << 2,
			Delete = 1 << 3,

			All = Select | Insert | Update | Delete
		}
		#endregion
	}
}
