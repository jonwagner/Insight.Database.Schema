using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Insight.Database.Schema
{
	/// <summary>
	/// Connects to the target database within the context of the installation.
	/// </summary>
	interface IDbInstallConnection
	{
		/// <summary>
		/// Executes a command against the database;
		/// </summary>
		/// <param name="sql">The sql to execute.</param>
		void ExecuteNonQuery(string sql);

		/// <summary>
		/// Gets a recordset from the database.
		/// </summary>
		/// <param name="sql">The sql to execute.</param>
		/// <returns>An open data reader.</returns>
		IDataReader GetDataReader(string sql);
	}
}
