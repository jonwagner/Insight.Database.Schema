using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Insight.Database.Schema.Implementation
{
	class Login : SchemaImpl
	{
		public Login(string name, string sql) : base(CleanupName(name), sql, 1)
		{
		}

		public override bool Exists(IDbConnection connection)
		{
			return 0 < connection.ExecuteScalarSql<int>(String.Format(@"
					SELECT COUNT (*)
						FROM sys.server_principals WHERE name = '{0}' AND type <> 'R'",
				Name.Object));
		}

		public override void Drop(IDbConnection connection)
		{
			connection.ExecuteSql(String.Format(@"DROP LOGIN {0}", Name.ObjectFormatted));
		}

		private static string CleanupName(string name)
		{
			return Regex.Match(name, @"LOGIN (?<name>.*)", RegexOptions.IgnoreCase).Groups["name"].Value;
		}
	}
}
