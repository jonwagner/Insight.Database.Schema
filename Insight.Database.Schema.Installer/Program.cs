using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Insight.Database.Schema.Installer
{
    class Program
    {
        static string ConnectionString = null;
        static string Server = ".";
        static string Database = null;
        static bool IntegratedSecurity = true;
        static string UserID = null;
        static string Password = null;
        static string SchemaGroup = "";
        static bool AllowRepair = false;
        static SchemaObjectCollection Schema = new SchemaObjectCollection();
        static bool Verbose = false;
        static bool Uninstall = false;

        static int Main(string[] args)
        {
            try
            {
                ProcessArguments(args);
            }
            catch (Exception e)
            {
				Console.WriteLine("Error detected:");
                Console.WriteLine(e.Message);
                if (Verbose)
                    Console.WriteLine(e.StackTrace);

				return 1;
            }

			return 0;
        }

        /// <summary>
        /// Process the command line arguments.
        /// </summary>
        /// <param name="args">The command line arguments.</param>
        static void ProcessArguments(string[] args)
        {
            int i = 0;

            if (args.Length == 0)
            {
                ShowHelp();
                return;
            }

            for (; i < args.Length; i++)
            {
                switch (args[i].ToUpperInvariant())
                {
                    case "-HELP":
                        ShowHelp();
                        return;

                    case "-C":
                    case "-CONNECTIONSTRING":
                        ConnectionString = args[++i];
                        break;

                    case "-S":
                    case "-SERVER":
                        Server = args[++i];
                        break;

                    case "-DB":
                    case "-DATABASE":
                        Database = args[++i];
                        break;

                    case "-U":
                    case "-USERID":
                        UserID = args[++i];
                        IntegratedSecurity = false;
                        break;
                        
                    case "-P":
                    case "-PASSWORD":
                        Password = args[++i];
                        IntegratedSecurity = false;
                        break;

                    case "-G":
                    case "-GROUP":
                        SchemaGroup = args[++i];
                        break;

                    case "-R":
                    case "-REPAIR":
                        AllowRepair = true;
                        break;

                    case "-V":
                    case "-VERBOSE":
                        Verbose = true;
                        break;

                    case "-UNINSTALL":
                        Uninstall = true;
                        break;

                    default:
                        AddSchema(args[i]);
                        break;
                }
            }

            // make sure that we have a connection string
            EnsureConnectionString();

            if (Schema.Count == 0 && !Uninstall)
                Console.WriteLine("The schema is empty. To uninstall a schema, specify the -uninstall option");

            Install();
        }

        /// <summary>
        /// Show the help for the tool.
        /// </summary>
        static void ShowHelp()
        {
            Console.WriteLine(@"
InsightInstaller - automatically upgrades SQL schemas.

Usage:
    InsightInstaller [options] [filespec1 filespec2...]

Examples:

    InsightInstaller -s proddb -db mydatabase *.sql

Parameters:

    Filespec can use wildcards. It can be .SQL files or assembly files with embedded resources (.dll or .exe).

Options:

    -help               : shows this help

    -c connectionstring : sets the connection string (or use -s -db -u -p)
    -s server           : sets the server (if -c not specified)
    -db database        : sets the database (if -c not specified)
    -u userid           : sets the username (default is integrated security)
    -p password         : sets the password (default is integrated security)

    -g schemagroup      : sets the schema group to install under (default is "")
    -r                  : enables repair mode
    -v                  : enables verbose errors

    -uninstall          : allows uninstall of an empty schema
");
        }

        /// <summary>
        /// Load a schema from a file or set of files.
        /// </summary>
        /// <param name="file">The path to the file(s).</param>
        /// <returns>True if the load operation was successful.</returns>
        static void AddSchema(string file)
        {
            string path = Path.GetDirectoryName(file);
            if (String.IsNullOrWhiteSpace(path))
                path = ".";
            string filespec = Path.GetFileName(file);

            foreach (string f in Directory.GetFiles(path, filespec))
            {
                Console.WriteLine("Loading {0}", f);

                string extension = Path.GetExtension(file).ToLower();
                if (extension == ".dll" || extension == ".exe")
                {
                    // support loading from assemblies
                    Assembly assembly = Assembly.LoadFile(Path.GetFullPath(f));
                    Schema.Load(assembly);
                }
                else
                {
                    Schema.Load(f);
                }
            }
        }

        /// <summary>
        /// Make sure that we have a connection string.
        /// </summary>
        static void EnsureConnectionString()
        {
            if (ConnectionString != null)
                return;

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = Server;
            builder.InitialCatalog = Database;
            builder.IntegratedSecurity = IntegratedSecurity;
            if (!String.IsNullOrWhiteSpace(UserID))
                builder.UserID = UserID;
            if (!String.IsNullOrWhiteSpace(Password))
                builder.Password = Password;

            ConnectionString = builder.ConnectionString;
        }

        /// <summary>
        /// Install the schema.
        /// </summary>
        static void Install()
        {
            SchemaInstaller.CreateDatabase(ConnectionString);

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                Console.WriteLine("Beginning install...");
                SchemaInstaller installer = new SchemaInstaller(connection);
                installer.AllowRepair = AllowRepair;
                new SchemaEventConsoleLogger().Attach(installer);
                installer.Install(SchemaGroup, Schema);
                Console.WriteLine("Done.");
            }
        }
    }
}
