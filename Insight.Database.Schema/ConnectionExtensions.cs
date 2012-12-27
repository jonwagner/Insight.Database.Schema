using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Insight.Database.Schema
{
	internal static class ConnectionExtensions
	{
		public static DbConnection Unwrap(this DbConnection connection)
		{
			RecordingDbConnection recording = connection as RecordingDbConnection;
			if (recording != null)
				return recording.InnerConnection.Unwrap();

			return connection;
		}

		public static int ExecuteSql(this IDbConnection connection, string sql)
		{
			var cmd = connection.CreateCommand();
			cmd.CommandText = sql;
			cmd.CommandType = CommandType.Text;
			cmd.Connection = connection;

			return cmd.ExecuteNonQuery();
		}

		public static T ExecuteScalarSql<T>(this IDbConnection connection, string sql, IDictionary<string, object> parameters)
		{
			var cmd = connection.CreateCommand();
			cmd.CommandText = sql;
			cmd.CommandType = CommandType.Text;
			cmd.Connection = connection;

			if (parameters != null)
				CreateParameters(parameters, cmd);

			return (T)cmd.ExecuteScalar();
		}

		public static IDataReader GetReaderSql(this IDbConnection connection, string sql)
		{
			var cmd = connection.CreateCommand();
			cmd.CommandText = sql;
			cmd.CommandType = CommandType.Text;
			cmd.Connection = connection;

			return cmd.ExecuteReader();
		}

		public static IList<ExpandoObject> QuerySql(this IDbConnection connection, string sql, IDictionary<string, object> parameters)
		{
			var cmd = connection.CreateCommand();
			cmd.CommandText = sql;
			cmd.CommandType = CommandType.Text;
			cmd.Connection = connection;

			CreateParameters(parameters, cmd);

			var results = new List<ExpandoObject>();

			using (var reader = cmd.ExecuteReader())
			{
				while (reader.Read())
				{
					var expando = new ExpandoObject();
					var dict = (IDictionary<string, object>)expando;
					for (int i = 0; i < reader.FieldCount; i++)
						dict[reader.GetName(i)] = reader.GetValue(i);
					results.Add(expando);
				}
			}

			return results;
		}

		private static void CreateParameters(IDictionary<string, object> parameters, IDbCommand cmd)
		{
			foreach (var pair in parameters)
			{
				var p = cmd.CreateParameter();
				p.ParameterName = "@" + pair.Key;
				p.Value = pair.Value;
				cmd.Parameters.Add(p);
			}
		}
	}
}
