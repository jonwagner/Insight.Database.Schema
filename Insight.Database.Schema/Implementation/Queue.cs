using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Insight.Database.Schema.Implementation
{
	class Queue : SchemaImpl
	{
		public Queue(string name, string sql) : base(CleanupName(name), sql, 1)
		{
		}

		public override bool Exists(IDbConnection connection)
		{
			return 0 < connection.ExecuteScalarSql<int>(String.Format(@"
					SELECT COUNT (*)
						FROM sys.service_queues
						WHERE name = '{0}'",
				Name.Object));
		}

		public override void Drop(IDbConnection connection)
		{
			connection.ExecuteSql(String.Format(@"DROP QUEUE {0}", Name.ObjectFormatted));
		}

		private static string CleanupName(string name)
		{
			return Regex.Match(name, @"QUEUE (?<name>.*)", RegexOptions.IgnoreCase).Groups["name"].Value;
		}
	}
}
