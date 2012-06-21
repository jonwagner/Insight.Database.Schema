using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Insight.Database.Schema;
using System.Reflection;
using System.Data.SqlClient;

namespace Insight.Database.Schema.Sample
{
	class Program
	{
		static void Main(string[] args)
		{
			SqlConnectionStringBuilder connection = new SqlConnectionStringBuilder("Database=.;Initial Catalog=InsightTest;Integrated Security=true");

			// make sure our database exists
			SchemaInstaller installer = new SchemaInstaller(connection.ConnectionString, connection.InitialCatalog);
			new SchemaEventConsoleLogger().Attach(installer);
			installer.CreateDatabase();

			// load the schema from the embedded resources in this project
			SchemaObjectCollection schema = new SchemaObjectCollection();
			schema.Load(Assembly.GetExecutingAssembly());

			// install the schema
			Console.WriteLine("Installing");
			installer.Install("BeerGarten", schema);

			// uninstall the schema
			if (args.Length > 0 && args[0].ToUpperInvariant() == "uninstall")
			{
				Console.WriteLine("Uninstalling");
				installer.Uninstall("BeerGarten");
			}
		}
	}
}
