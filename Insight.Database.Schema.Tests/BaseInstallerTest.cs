using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace Insight.Database.Schema.Tests
{
	/// <summary>
	/// Base test case support classes
	/// </summary>
	public class BaseInstallerTest
	{
		/// <summary>
		/// The list of connection strings that will be used to run tests.
		/// </summary>
		protected IEnumerable<string> ConnectionStrings { get { return _connectionStrings; } }
		private static readonly ReadOnlyCollection<string> _connectionStrings = new ReadOnlyCollection<string>(
			ConfigurationManager.ConnectionStrings.OfType<ConnectionStringSettings>()
				.Where(c => c.Name.Contains("Test"))
				.Select(c => c.ConnectionString)
				.ToList());

		/// <summary>
		/// The schema group to use for the test cases.
		/// </summary>
		protected static string TestSchemaGroup = "test";

		/// <summary>
		/// Run a test and clean up the databases when complete.
		/// </summary>
		/// <param name="connectionString">The connection string for the database.</param>
		/// <param name="action">The test to run.</param>
		protected static void TestWithDrop(string connectionString, Action action)
		{
			try
			{
				action();
			}
			finally
			{
				SchemaInstaller.DropDatabase(connectionString);
			}
		}

		/// <summary>
		/// Run a test and clean up the databases when complete.
		/// </summary>
		/// <param name="connectionString">The connection string for the database.</param>
		/// <param name="action">The test to run.</param>
		internal static void TestWithRollback(string connectionString, Action<RecordingDbConnection> action)
		{
			// make sure the database exists
			if (!SchemaInstaller.DatabaseExists(connectionString))
				SchemaInstaller.CreateDatabase(connectionString);

			// do all of the work in a transaction so we can clean up our changes
			using (TransactionScope transaction = new TransactionScope())
			using (SqlConnection connection = new SqlConnection(connectionString))
			using (RecordingDbConnection recordingConnection = new RecordingDbConnection(connection))
			{
				recordingConnection.Open();
				try
				{
					action(recordingConnection);
				}
				finally
				{
					Console.WriteLine("== BEGIN SCRIPT ============================");
					Console.WriteLine(recordingConnection.ScriptLog.ToString());
					Console.WriteLine("== END SCRIPT ============================");
				}
			}
		}
	}
}
