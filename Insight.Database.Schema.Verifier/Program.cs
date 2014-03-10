using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Text.RegularExpressions;

namespace Insight.Database.Schema.Verifier
{
	/// <summary>
	/// Loads the given schema from a SQL file (or files), and runs tests on them to make sure that everything works all good.
	/// </summary>
	class Program
	{
		static bool ScriptOnly = false;
		static bool Clean = false;
		static bool SMOTest = false;
		static int Skip = 0;
		static int Take = Int32.MaxValue;
		static string Filename;
		static string Server = ".";
		static string Database = "InsightTestVerify";
		static SchemaObjectType? TypeFilter = null;
		static SqlConnectionStringBuilder ConnectionString = new SqlConnectionStringBuilder();

		static void Main(string[] args)
		{
			for (int i = 0; i < args.Length; i++)
			{
				switch (args[i].ToUpperInvariant())
				{
					case "-SERVER":
						Server = args[++i];
						break;

					case "-DATABASE":
						Database = args[++i];
						break;
					
					case "-SKIPTO":
						Skip = Int32.Parse(args[++i]) - 1;
						break;

					case "-TAKE":
						Take = Int32.Parse(args[++i]);
						break;

					case "-CLEAN":
						Clean = true;
						break;

					case "-FILTER":
						TypeFilter = (SchemaObjectType)Enum.Parse(typeof(SchemaObjectType), args[++i]);
						break;

					case "-SMO":
						SMOTest = true;
						break;

					case "-SCRIPT":
						ScriptOnly = true;
						break;

					default:
						if (Filename == null)
							Filename = args[i];
						break;
				}				
			}

			// set up the connection string
			ConnectionString.InitialCatalog = Database;
			ConnectionString.DataSource = Server;
			ConnectionString.IntegratedSecurity = true;

			// drop the database if starting clean
			if (Clean)
				SchemaInstaller.DropDatabase(ConnectionString.ConnectionString);

			// make sure we are always working with an empty database
			if (!CreateDatabase())
				return;

			// load the schema
			SchemaObjectCollection schema = LoadSchema();

			try
			{
				// install the schema as is
				using (SqlConnection connection = new SqlConnection(ConnectionString.ConnectionString))
				{
					connection.Open();

					// install it the first time
					SchemaInstaller installer = new SchemaInstaller(connection);
					installer.Install("Test", schema);
					schema.Verify(connection);

					// script the result through SMO
					Console.WriteLine("Scripting database");
					List<string> originalScript = null;
					if (SMOTest)
					{
						originalScript = ScriptDatabaseWithSMO().ToList();
						originalScript.Sort();
					}

					//  run test cases that modify each of the elements
					for (int i = Skip; i < schema.Count; i++)
					{
						if (Take-- <= 0)
							return;

						// if a type filter is defined, then filter by that type
						if (TypeFilter.HasValue && schema[i].SchemaObjectType != TypeFilter.Value)
							continue;

						// if the type can't be modified, then don't test it
						if (!schema[i].CanModify(connection))
						{
							Console.WriteLine();
							Console.WriteLine("Not testing modification of {0} {1}", schema[i].SchemaObjectType, schema[i].SqlName.FullName);
							continue;
						}

						// make sure all of the objects are there
						try
						{
							Console.Write('\r');
							Console.Write(new String(' ', Console.WindowWidth - 1));
							Console.Write('\r');
							Console.Write("Testing modifications {0}/{1}", (i + 1), schema.Count);

							// modify the schema and re-install it
							schema[i] = new SchemaObject(schema[i].Sql + " -- MODIFIED");

							if (ScriptOnly)
								Console.WriteLine(installer.ScriptChanges("Test", schema));
							else
								installer.Install("Test", schema);

							// make sure all of the objects are there
							if (SMOTest)
							{
								// script the whole database
								var updatedScript = ScriptDatabaseWithSMO().ToList();
								updatedScript.Sort();
								MatchScripts(originalScript, updatedScript);
							}
							else
							{
								// just verify the dependencies
								schema.Verify(connection);
							}
						}
						catch (Exception e)
						{
							Console.WriteLine();
							Console.WriteLine("ERROR While modifying:");
							Console.WriteLine(schema[i].Name);
							Console.WriteLine(e.ToString());

							throw;
						}
					}

					Console.WriteLine();
				}
			}
			finally
			{
				Console.WriteLine("Dropping database");
				SchemaInstaller.DropDatabase(ConnectionString.ConnectionString);
			}
		}

		/// <summary>
		/// Load the schema from the file(s) that the user specified.
		/// </summary>
		static SchemaObjectCollection LoadSchema()
		{
			SchemaObjectCollection schema = new SchemaObjectCollection();

			// convert the filename to a search spec
			string path = Path.GetDirectoryName(Filename);
			string pattern = Path.GetFileName(Filename);
			if (path == pattern)
				path = Directory.GetCurrentDirectory();

			foreach (var file in Directory.EnumerateFiles(path, pattern))
				schema.Load(file);

			return schema;
		}

		/// <summary>
		/// Make sure the database exists.
		/// </summary>
		/// <returns></returns>
		static bool CreateDatabase()
		{
			// make sure the database does not exist
			if (SchemaInstaller.DatabaseExists(ConnectionString.ConnectionString))
			{
				Console.WriteLine("The database {0} already exists. Verify needs an empty database.", Database);
				return false;
			}

			// create the database
			Console.WriteLine("Creating database");
			SchemaInstaller.CreateDatabase(ConnectionString.ConnectionString);

			return true;
		}

		/// <summary>
		/// Returns the script used to re-create the database.
		/// </summary>
		/// <returns>A list of scripts used to re-create the database.</returns>
		static List<string> ScriptDatabaseWithSMO()
		{
			if (!SMOTest)
				return new List<string>();

			Server server = new Server(ConnectionString.DataSource);
			var database = server.Databases[ConnectionString.InitialCatalog];
			Transfer transfer = new Transfer(database)
			{
				CopyAllDatabaseTriggers = true,
				CopyAllDefaults = true,
				CopyAllFullTextCatalogs = true,
				CopyAllFullTextStopLists = true,
				CopyAllLogins = true,
				CopyAllObjects = true,
				CopyAllPartitionFunctions = true,
				CopyAllPartitionSchemes = true,
				CopyAllPlanGuides = true,
				CopyAllRoles = true,
				CopyAllRules = true,
				CopyAllSchemas = true,
				CopyAllSearchPropertyLists = true,
				CopyAllSequences = true,
				CopyAllSqlAssemblies = true,
				CopyAllStoredProcedures = true,
				CopyAllSynonyms = true,
				CopyAllTables = true,
				CopyAllUserDefinedAggregates = true,
				CopyAllUserDefinedDataTypes = true,
				CopyAllUserDefinedFunctions = true,
				CopyAllUserDefinedTableTypes = true,
				CopyAllUserDefinedTypes = true,
				CopyAllUsers = true,
				CopyAllViews = true,
				CopyAllXmlSchemaCollections = true,
				CopyData = false,
				CopySchema = true,
				CreateTargetDatabase = false,
			};

			// save the script for use later
			return transfer.EnumScriptTransfer().ToList();
		}

		static void MatchScripts(IList<string> original, IList<string> modified)
		{
			int scripts = Math.Min(original.Count, modified.Count);

			Regex passwordRegex = new Regex("PASSWORD=N'(('')|[^'])*'");

			for (int i = 0; i < scripts; i++)
			{
				string o = original[i];
				o = passwordRegex.Replace(o, "");
				string m = original[i];
				m = passwordRegex.Replace(m, "");
				
				if (o != m)
					throw new ApplicationException(String.Format("Scripts do not match. Original:\n{0}\nModified:\n{1}", original[i], modified[i]));
			}
		}
	}
}
