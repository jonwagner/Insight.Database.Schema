#region Using directives
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Transactions;

using Insight.Database.Schema.Properties;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
#endregion

namespace Insight.Database.Schema
{
    #region SchemaInstaller Class
    /// <summary>
    /// Installs, upgrades, and uninstalls objects from a database
    /// </summary>
    public sealed class SchemaInstaller : IDisposable, IDbInstallConnection
    {
        #region Constructors
        /// <summary>
        /// Create a SchemaInstaller that is connected to a given database
        /// </summary>
        /// <param name="connectionString">A connection string to the server. If the database does not exist, then a connection to the master database should be used.</param>
        /// <param name="databaseName">The name of the database to edit</param>
        /// <exception cref="ArgumentNullException">If connectionString is null</exception>
        /// <exception cref="SqlException">If the database connection cannot be established</exception>
        /// <exception cref="ArgumentException">If the database name contains invalid characters</exception>
        /// <remarks>The database connection is held open for the lifetime of the SchemaInstaller.</remarks>
        public SchemaInstaller (string connectionString, string databaseName)
        {
            // we need a connection string
            if (connectionString == null) throw new ArgumentNullException ("connectionString");
            if (databaseName == null) throw new ArgumentNullException ("databaseName");
            AssertValidSqlName (databaseName);

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder (connectionString);
            builder.Pooling = false;

			// save the connection string
			builder.InitialCatalog = databaseName;
			_connectionString = builder.ConnectionString;
            _databaseName = databaseName;

			// save the master connection string
			builder.InitialCatalog = "master";
			_masterConnectionString = builder.ConnectionString;
        }
        #endregion

        #region Database Utility Methods
        /// <summary>
        /// Create a database on the specified connection if it does not exist
        /// </summary>
        /// <returns>True if the database was created, false if it already exists</returns>
        /// <exception cref="SqlException">If the database name is invalid</exception>
		public bool CreateDatabase ()
        {
			using (_connection = new SqlConnection(_masterConnectionString))
            {
                _connection.Open ();

                // see if the database already exists
                bool createDatabase = !DatabaseExists ();

                // only create the database if it doesn't exist
                if (createDatabase)
                    ExecuteNonQuery (String.Format (CultureInfo.InvariantCulture, "CREATE DATABASE [{0}]", _databaseName));

                return createDatabase;
            }
        }

        /// <summary>
        /// Drop a database if it exists
        /// </summary>
        /// <returns>True if the database was dropped, false if it did not exist</returns>
        /// <exception cref="SqlException">If the database name is invalid or cannot be dropped</exception>
        public bool DropDatabase ()
        {
			using (_connection = new SqlConnection(_masterConnectionString))
            {
                _connection.Open ();

                // if database does not exist, then don't delete it
                if (!DatabaseExists())
                    return false;

                // set the database to single user mode, effectively dropping all connections except the current
                // connection.
                ExecuteNonQuery(String.Format(CultureInfo.InvariantCulture, "exec sp_dboption N'{0}', N'single', N'true'", _databaseName));

                // drop the database
                ExecuteNonQuery (String.Format (CultureInfo.InvariantCulture, "DROP DATABASE [{0}]", _databaseName));
                return true;
            }
        }
    	#endregion  

        #region Schema Installation Methods
        /// <summary>
        /// Install a set of schema objects into the database
        /// </summary>
        /// <remarks>
        /// This is a transactional operation.
        /// Also, note that although the names of schema objects are validated to not contain insecure characters,
        /// the scripts that are installed are not checked.
        /// </remarks>
        /// <param name="schemaGroup">The schema group to install objects into</param>
        /// <param name="objects">The list of schema objects to install</param>
        /// <exception cref="ArgumentException">If object names are not unique</exception>
        /// <exception cref="ArgumentNullException">If schemaGroup is null</exception>
        /// <exception cref="ArgumentNullException">If objects is null</exception>
        /// <exception cref="SqlException">If any object fails to install</exception>
        /// <exception cref="ArgumentException">If a parameter contains an invalid SQL character</exception>
		public string Install (string schemaGroup, SchemaObjectCollection objects)
		{
			return Install (schemaGroup, objects, false);
		}

        public string Install (string schemaGroup, SchemaObjectCollection objects, bool rebuild)
		{
			return Install (schemaGroup, objects, rebuild ? RebuildMode.RebuildFull : RebuildMode.DetectChanges);
		}

        public string Install (string schemaGroup, SchemaObjectCollection objects, RebuildMode rebuildMode)
        {
			_scripts = new StringBuilder ();

            // validate the arguments
            if (schemaGroup == null) throw new ArgumentNullException ("schemaGroup");
            if (objects == null) throw new ArgumentNullException ("objects");

            // get the list of objects
            List<SchemaObject> schemaObjects = new List<SchemaObject> (objects);
            ValidateSchemaObjects (schemaObjects);
            for (int i = 0; i < objects.Count; i++)
                objects[i].OriginalOrder = i;

            // sort the list of objects in installation order
            schemaObjects.Sort (delegate (SchemaObject o1, SchemaObject o2) 
            { 
                int compare = o1.SchemaObjectType.CompareTo (o2.SchemaObjectType);
                if (compare == 0)
                    compare = o1.OriginalOrder.CompareTo (o2.OriginalOrder);
                if (compare == 0)
					compare = String.Compare(o1.Name, o2.Name, StringComparison.OrdinalIgnoreCase);
				return compare;
            });

			// the schema changes must be done in a transaction
            // since we don't pool the connection, we need to end the transaction before closing the connection
            try
            {
                using (TransactionScope transaction = new TransactionScope (TransactionScopeOption.Required, new TimeSpan (1, 0, 0, 0, 0)))
                {
					// open the connection
					OpenConnection ();

					 // make sure we have a schema registry
					SchemaRegistry registry = new SchemaRegistry (_connection);

                    // keep a list of all of the operations we need to perform
                    List<string> dropObjects = new List<string> ();
                    List<SchemaObject> addObjects = new List<SchemaObject> ();
                    List<SchemaObject> tableUpdates = new List<SchemaObject> ();

                    // look through all of the existing objects in the registry
                    // create a delete instruction for all of the ones that should no longer be there
                    foreach (string objectName in registry.GetObjectNames (schemaGroup))
                    {
                        SchemaObject schemaObject = schemaObjects.Find (delegate (SchemaObject o) 
						{
							return (o.Name.ToUpperInvariant() == objectName.ToUpperInvariant());
						});
                        if (schemaObject == null)
                            dropObjects.Add (objectName);
                    }

					// sort to drop in reverse dependency order 
					dropObjects.Sort (delegate (string o1, string o2)
					{
						int compare = -registry.GetObjectType (o1).CompareTo (registry.GetObjectType (o2));
						if (compare == 0)
							compare = -registry.GetOriginalOrder(o1).CompareTo(registry.GetOriginalOrder(o2)); 
						if (compare == 0)
							compare = -String.Compare(o1, o2, StringComparison.OrdinalIgnoreCase);

						return compare;
					});

                    // find out if we need to add anything
                    foreach (SchemaObject schemaObject in schemaObjects)
                    {
                        // add any objects that aren't in the registry yet
                        if (!registry.Contains (schemaObject.Name))
                            addObjects.Add (schemaObject);
                    }

					// see if there are any drops or modifications
					bool hasChanges = dropObjects.Count != 0;
					if (!hasChanges)
					{
						foreach (SchemaObject schemaObject in schemaObjects)
						{
							if (registry.Contains (schemaObject.Name) &&
								registry.GetSignature(schemaObject.Name) != schemaObject.GetSignature(this, objects))
							{
								hasChanges = true;
								break;
							}
						}
					}

					// if there are changes, drop all of the easy items
					// drop and re-add all of the easy items
					if (hasChanges || (rebuildMode > RebuildMode.DetectChanges))
					{
						for (int i = schemaObjects.Count - 1; i >= 0; i--)
						{
							SchemaObject schemaObject = schemaObjects [i];
							if (registry.Contains (schemaObject.Name) &&
								(
									IsEasyToModify (schemaObject.SchemaObjectType) || 
									(rebuildMode >= RebuildMode.RebuildSafe && CanRebuildSafely (schemaObject.SchemaObjectType)) ||
									(rebuildMode >= RebuildMode.RebuildFull && CanRebuild (schemaObject.SchemaObjectType))
								) &&
								!dropObjects.Contains (schemaObject.Name))
							{
								dropObjects.Add (schemaObject.Name);
								addObjects.Add (schemaObject);
							}
						}
					}

					// drop and re-add everything else, using the scripting engine
					for (int i = schemaObjects.Count - 1; i >= 0; i--)
					{
						SchemaObject schemaObject = schemaObjects [i];

						if (registry.Contains (schemaObject.Name) && 
							registry.GetSignature (schemaObject.Name) != schemaObject.GetSignature(this, objects) &&
							!IsEasyToModify (schemaObject.SchemaObjectType) && 
							!dropObjects.Contains (schemaObject.Name))
                            ScheduleUpdate (dropObjects, addObjects, tableUpdates, schemaObject, true);
					}

					// sort to add in dependency order
					addObjects.Sort (delegate (SchemaObject o1, SchemaObject o2)
					{
						int compare = o1.SchemaObjectType.CompareTo (o2.SchemaObjectType);
						if (compare == 0)
							compare = o1.OriginalOrder.CompareTo (o2.OriginalOrder);
						if (compare == 0)
							compare = String.Compare (o1.Name, o2.Name, StringComparison.OrdinalIgnoreCase);
						return compare;
					});

                    // do the work
                    DropObjects (registry, dropObjects, addObjects);
                    UpdateTables (schemaGroup, registry, addObjects, tableUpdates, objects);
					CreateObjects(schemaGroup, registry, addObjects, objects);
					VerifyObjects (schemaObjects);

					// update the sigs on all of the records
					foreach (SchemaObject o in schemaObjects)
						registry.UpdateObject(o, schemaGroup, this, objects);

                    // commit the changes
                    registry.Update ();
                    transaction.Complete();
                }
            }
            finally
            {
                _connection.Dispose ();
            }

			return _scripts.ToString ();
        }

		public bool Diff (string schemaGroup, SchemaObjectCollection schemaObjects)
		{
			try
			{
				OpenConnection ();
				SchemaRegistry registry = new SchemaRegistry (_connection);

				// drop and re-add everything else, using the scripting engine
				foreach (SchemaObject schemaObject in schemaObjects)
				{
					// if the registry is missing the object, it's new, that's a diff
					if (!registry.Contains (schemaObject.Name))
						return true;

					// if the signatures don't match, that's a diff
					if (registry.GetSignature(schemaObject.Name) != schemaObject.GetSignature(this, schemaObjects))
						return true;
				}

				// look through all of the existing objects in the registry
				// create a delete instruction for all of the ones that should no longer be there
				foreach (string objectName in registry.GetObjectNames (schemaGroup))
				{
					SchemaObject schemaObject = schemaObjects.FirstOrDefault (delegate (SchemaObject o)
					{
						return (o.Name.ToUpperInvariant() == objectName.ToUpperInvariant());
					});
					if (schemaObject == null)
						return true;
				}

				// didn't detect differences
				return false;
			}
			finally
			{
				_connection.Dispose ();
			}
		}

		private static bool IsEasyToModify (SchemaObjectType schemaObjectType)
		{
			switch (schemaObjectType)
			{
				case SchemaObjectType.View:
				case SchemaObjectType.Function:
				case SchemaObjectType.StoredProcedure:
				case SchemaObjectType.Permission:
				case SchemaObjectType.Trigger:
					return true;
			}

			return false;
		}

		private static bool CanRebuildSafely(SchemaObjectType schemaObjectType)
		{
			if (IsEasyToModify (schemaObjectType))
				return true;

			switch (schemaObjectType)
			{
				case SchemaObjectType.Constraint:
				case SchemaObjectType.ForeignKey:
					return true;
			}

			return false;
		}

		private static bool CanRebuild(SchemaObjectType schemaObjectType)
		{
			if (CanRebuildSafely (schemaObjectType))
				return true;

			switch (schemaObjectType)
			{
				case SchemaObjectType.IndexedView:
				case SchemaObjectType.Index:
				case SchemaObjectType.PrimaryKey:
				case SchemaObjectType.PrimaryXmlIndex:
				case SchemaObjectType.SecondaryXmlIndex:
				case SchemaObjectType.UserScript:
					return true;
			}

			return false;
		}
		
		/// <summary>
		/// Perform a dry run by running the install and rolling back
		/// </summary>
		public string DryRun (string schemaGroup, SchemaObjectCollection objects)
		{
			using (TransactionScope transaction = new TransactionScope (TransactionScopeOption.Required, new TimeSpan ()))
			{
				return Install (schemaGroup, objects);
			}
		}

		private StringBuilder _scripts;

        /// <summary>
        /// Schedule an update by adding the appropriate delete, update and add records
        /// </summary>
        /// <param name="dropObjects">The list of tdrops</param>
        /// <param name="addObjects">The list of adds</param>
        /// <param name="tableUpdates">The list of table updates</param>
        /// <param name="schemaObject">The object to update</param>
        private void ScheduleUpdate (List<string> dropObjects, List<SchemaObject> addObjects, List<SchemaObject> tableUpdates, SchemaObject schemaObject, bool handleDependencies)
        {
            // if the object is a table, we need to update it
            if (schemaObject.SchemaObjectType == SchemaObjectType.Table)
                tableUpdates.Add (schemaObject);
            else
            {
                // not a table, so add a drop and insert
                // put the add in before scripting the permissions so the permissions execute after the add
                addObjects.Add (schemaObject);
				if (handleDependencies)
				{
					switch (schemaObject.SchemaObjectType)
					{
						case SchemaObjectType.StoredProcedure:
							StoredProcedure sp = _database.StoredProcedures [schemaObject.UnformattedName];
							if (sp != null)
								ScriptPermissions (sp.Urn, addObjects);
							break;

						case SchemaObjectType.Function:
							UserDefinedFunction udf = _database.UserDefinedFunctions [schemaObject.UnformattedName];
							if (udf != null)
								ScriptPermissions (udf.Urn, addObjects);
							break;

						case SchemaObjectType.View:
							View view = _database.Views [schemaObject.UnformattedName];
							if (view != null)
								ScriptPermissions (view.Urn, addObjects);
							break;
					}
                }

                // insert at the beginning so the higher level objects get dropped before their dependencies
				dropObjects.Add (schemaObject.Name);
            }
        }

        private static void ValidateSchemaObjects (List<SchemaObject> schemaObjects)
        {
            // check for duplicate objects and invalid names
            foreach (SchemaObject schemaObject in schemaObjects)
            {
                AssertValidSqlName (schemaObject.Name);
                if (schemaObjects.FindAll (delegate (SchemaObject o) { return o.Name == schemaObject.Name; }).Count > 1)
                    throw new ArgumentException (String.Format (CultureInfo.InvariantCulture, Properties.Resources.DuplicateObjectName, schemaObject.Name));
            }
        }

        private void DropObjects (SchemaRegistry registry, List<string> dropObjects, List<SchemaObject> addObjects)
        {
            // drop objects
            foreach (string objectName in dropObjects)
            {
                if (DroppingObject != null)
                    DroppingObject (this, new SchemaEventArgs (SchemaEventType.BeforeDrop, objectName));

                // drop any table dependencies, if any
                SchemaObjectType type = registry.GetObjectType (objectName);
                switch (type)
                {
					case SchemaObjectType.UserDefinedType:
						DropTypeDependencies(objectName, addObjects);
						break;

					case SchemaObjectType.View:
						DropViewDependencies (objectName, addObjects);
						break;

                    case SchemaObjectType.Table:
						DropTableDepencencies(objectName, null, TableScriptOptions.IncludeTableModifiers | TableScriptOptions.AllXmlIndexes, true);
                        break;

                    case SchemaObjectType.PrimaryKey:
						DropTableDepencencies(SchemaObject.TableNameFromIndexName(objectName), addObjects, TableScriptOptions.AddAtEnd | TableScriptOptions.AllXmlIndexes, false);
                        break;

                    case SchemaObjectType.PrimaryXmlIndex:
						DropTableDepencencies(SchemaObject.TableNameFromIndexName(objectName), addObjects, TableScriptOptions.AddAtEnd | TableScriptOptions.SecondaryXmlIndexes, false);
                        break;
                }

                SchemaObject.Drop (this, _connection, type, objectName);
                registry.DeleteObject (objectName);
				ResetScripter ();
            }
        }

        private void UpdateTables (string schemaGroup, SchemaRegistry registry, List<SchemaObject> addObjects, List<SchemaObject> tableUpdates, SchemaObjectCollection objects)
        {
            foreach (SchemaObject schemaObject in tableUpdates)
            {
                if (UpdatingTable != null)
                    UpdatingTable (this, new SchemaEventArgs (SchemaEventType.BeforeTableUpdate, schemaObject));

				DropTableDepencencies(schemaObject.Name, addObjects, TableScriptOptions.IncludeTableModifiers | TableScriptOptions.AllXmlIndexes, true);

                // signature has changed, so update the object
                UpdateTable (_connection, schemaObject, objects);
                registry.UpdateObject (schemaObject, schemaGroup, this, objects);

                if (UpdatedTable != null)
                    UpdatedTable (this, new SchemaEventArgs (SchemaEventType.AfterTableUpdate, schemaObject));
            }
        }

        private void CreateObjects (string schemaGroup, SchemaRegistry registry, List<SchemaObject> addObjects, SchemaObjectCollection objects)
        {
            // create objects
            foreach (SchemaObject schemaObject in addObjects)
            {
                if (CreatingObject != null)
                    CreatingObject (this, new SchemaEventArgs (SchemaEventType.BeforeCreate, schemaObject));

				schemaObject.Install(this, objects);

                if (schemaObject.SchemaObjectType != SchemaObjectType.Script)
					registry.UpdateObject(schemaObject, schemaGroup, this, objects);

                if (CreatedObject != null)
                    CreatedObject (this, new SchemaEventArgs (SchemaEventType.AfterCreate, schemaObject));
            }
        }

		/// <summary>
		/// Verify that all of the objects that are supposed to be there really are...
		/// </summary>
		/// <param name="schemaObjects">The objects</param>
		private void VerifyObjects (List<SchemaObject> schemaObjects)
		{
            // create objects
			foreach (SchemaObject schemaObject in schemaObjects)
				schemaObject.Verify(this, _connection, schemaObjects);
		}

        #region Table Update Methods
        /// <summary>
        /// Update a table object
        /// </summary>
        /// <param name="connection">The SqlConnection to use</param>
        /// <remarks>This creates a copy of the table data, updates the table, then copies the data back into the new table.</remarks>
        private void UpdateTable (SqlConnection connection, SchemaObject schemaObject, SchemaObjectCollection objects)
        {
            // copy the table to a temp table and drop the old table
            // NOTE: sp_rename can't be used because we're in a distributed transaction
            SqlCommand command = new SqlCommand ();
            command.Connection = connection;
            //command.CommandText = String.Format (CultureInfo.InvariantCulture, "SELECT * INTO {0} FROM {1}; DROP TABLE {1}", TempTableName, schemaObject.Name);
			command.CommandText = String.Format (CultureInfo.InvariantCulture, "sp_rename '{1}', '{0}'", TempTableName, schemaObject.Name);
			command.CommandTimeout = 0;
            command.ExecuteNonQuery ();

            // create the new table using the script provided
			schemaObject.Install(this, objects);

            if (!DoConvertTable (schemaObject, TempTableName, schemaObject.Name))
            {
                // get the columns for the two tables
				command.CommandText = String.Format (CultureInfo.InvariantCulture, @"
					select c.*, type_name = t.name from sys.columns c join sys.types t on (c.system_type_id = t.system_type_id and c.user_type_id = t.user_type_id) where object_id = object_id ('{0}');
					select c.*, type_name = t.name from sys.columns c join sys.types t on (c.system_type_id = t.system_type_id and c.user_type_id = t.user_type_id) where object_id = object_id ('{1}');
				", TempTableName, schemaObject.UnformattedName);
                SqlDataAdapter adapter = new SqlDataAdapter ();
                adapter.SelectCommand = command;
                DataSet dataset = new DataSet ();
                dataset.Locale = CultureInfo.InvariantCulture;
                adapter.Fill (dataset);

                // find all of the fields that match from the old table to the new table
                // for these fields, we'll preserve the data
                StringBuilder newFields = new StringBuilder ();
                StringBuilder oldFields = new StringBuilder ();
                foreach (DataRow newRow in dataset.Tables[1].Rows)
                {
                    string columnName = newRow["name"].ToString ();

                    // don't map over timestamps and computed columns, they get done automatically
                    if (newRow["type_name"].ToString () == "timestamp")
                        continue;
					if (Convert.ToInt32 (newRow ["is_computed"], CultureInfo.InvariantCulture) == 1)
						continue;

                    // find a matching oldRow
                    DataRow[] oldRows = dataset.Tables[0].Select (String.Format (CultureInfo.InvariantCulture, "name = '{0}'", columnName));

                    // map old fields to new fields
                    if (oldRows.Length == 1)
                    {
                        if (newFields.Length > 0) newFields.Append (",");
                        newFields.AppendFormat ("[{0}]", columnName);

                        if (oldFields.Length > 0) oldFields.Append (",");
                        oldFields.AppendFormat ("[{0}]", columnName);
                    }
                }

                // copy the data into the new table
                command.CommandText = String.Format (CultureInfo.InvariantCulture, 
                    @"  IF OBJECTPROPERTY (OBJECT_ID('{0}'), 'TableHasIdentity') = 1 
                            SET IDENTITY_INSERT {0} ON; 
                        INSERT INTO {0} ({1}) SELECT {2} FROM {3}; 
                        IF OBJECTPROPERTY (OBJECT_ID('{0}'), 'TableHasIdentity') = 1 
                            SET IDENTITY_INSERT {0} OFF", 
                    schemaObject.Name, newFields, oldFields, TempTableName);
                command.ExecuteNonQuery ();
            }

            // drop the temp table
            command.CommandText = String.Format (CultureInfo.InvariantCulture, "DROP TABLE {0}", TempTableName);
            command.ExecuteNonQuery ();
        }

        private bool DoConvertTable (SchemaObject table, string beforeTable, string afterTable)
        {
            if (ConvertingTable != null)
            {
                ConvertTableEventArgs ce = new ConvertTableEventArgs (SchemaEventType.ConvertTable, table, _connection, beforeTable, afterTable);
                ConvertingTable (this, ce);
                return ce.Converted;
            }
            return false;
        }

        /// <summary>
        /// The name of the temporary table to use
        /// </summary>
        public static readonly string TempTableName = "Insight_tmpTable";
        #endregion

        /// <summary>
        /// Uninstall a schema group from the database.
        /// </summary>
        /// <remarks>This is a transactional operation</remarks>
        /// <param name="schemaGroup">The group to uninstall</param>
        /// <exception cref="ArgumentNullException">If schemaGroup is null</exception>
        /// <exception cref="SqlException">If any object fails to uninstall</exception>
        public void Uninstall (string schemaGroup)
        {
            // validate the arguments
            if (schemaGroup == null) throw new ArgumentNullException ("schemaGroup");

            // the schema changes must be done in a transaction
            try
            {
                using (TransactionScope transaction = new TransactionScope ())
                {
                    // open the connection
                    OpenConnection ();

                    // make sure we have a schema registry
                    SchemaRegistry registry = new SchemaRegistry (_connection);

                    // sort the objects in drop order (reverse create order)
                    List<string> names = registry.GetObjectNames (schemaGroup);
                    names.Sort (delegate (string n1, string n2)
                    {
                        return -registry.GetObjectType (n1).CompareTo (registry.GetObjectType (n2));
                    });

                    // delete any objects that are in the specified schema group
                    foreach (string objectName in names)
                    {
                        if (DroppingObject != null)
                            DroppingObject (this, new SchemaEventArgs (SchemaEventType.BeforeDrop, objectName));

                        SchemaObjectType type = registry.GetObjectType (objectName);
                        if (type == SchemaObjectType.Table)
							DropTableDepencencies(objectName, null, TableScriptOptions.IncludeTableModifiers, true);
                        SchemaObject.Drop (this, _connection, type, objectName);
                        registry.DeleteObject (objectName);
                    }

                    // commit the changes
                    registry.Update ();
                    transaction.Complete();
                }
            }
            finally
            {
                _connection.Dispose ();
            }
        }
        #endregion

        #region Scripting Methods
        /// <summary>
        /// Specifies how table scripting should be done
        /// </summary>
        [Flags]
        enum TableScriptOptions
        {
            /// <summary>
            /// Script the constraints on the table itself
            /// </summary>
            IncludeTableModifiers = 1 << 0,

            /// <summary>
            /// Add the changes to the end of the list, not the beginning
            /// </summary>
            AddAtEnd = 1 << 1,

            /// <summary>
            /// Are we scripting the existing table or a table referencing our table
            /// </summary>
            ScriptAnonymousConstraints = 1 << 2,

            /// <summary>
            /// Script Primary Xml Indexes
            /// </summary>
            PrimaryXmlIndexes = 1 << 3,

            /// <summary>
            /// Script Secondary Xml Indexes
            /// </summary>
            SecondaryXmlIndexes = 1 << 4,

            /// <summary>
            /// All Xml Indexes
            /// </summary>
            AllXmlIndexes = PrimaryXmlIndexes | SecondaryXmlIndexes,
        }

        /// <summary>
        /// Drops all of the dependencies on a table so the table can be updated.
        /// </summary>
        /// <param name="tableName">The table to update</param>
        /// <param name="addObjects">The list of addObjects, or null to not re-add dependencies</param>
        /// <param name="tableUpdates">The list of updated being made to tables. Need to prevent duplicate changes</param>
        /// <param name="options">Options for scripting</param>
        /// <remarks>Drops the dependencies and adds SchemaObjects to readd the dependencies later</remarks>
		private void DropTableDepencencies(string tableName, List<SchemaObject> addObjects, TableScriptOptions options, bool modifyingTable)
        {
            Table table = _database.Tables[SchemaObject.UnformatSqlName (tableName)];
            if (table == null)
                return;

            DependencyTree tree = _scripter.DiscoverDependencies (new SqlSmoObject[] { table }, DependencyType.Children);

            // find all of the tables that refer to this table and drop all of the foreign keys
            for (DependencyTreeNode dependent = tree.FirstChild.FirstChild; dependent != null; dependent = dependent.NextSibling)
            {
				if (dependent.Urn.Type == "Table")
				{
					// script the re-add of foreign keys, but not the tables themselves
					_scripter.Options = new ScriptingOptions (ScriptOption.DriForeignKeys) - ScriptOption.PrimaryObject;

					// don't script anonymous constraints.
					// if they are on the table, they will get created with the new table
					// if they are not on the table, they should go away
					options &= ~TableScriptOptions.ScriptAnonymousConstraints;

					DropAndReAdd (dependent.Urn, addObjects, options);
				}
				else
				if (modifyingTable && dependent.Urn.Type == "View")
				{
					DropViewDependencies (dependent.Urn, addObjects);
					DropAndReAdd (dependent.Urn, addObjects, options);
				}
            }

            // handle xml indexes separately
            if ((options & TableScriptOptions.AllXmlIndexes) != 0)
            {
                // drop all of the permissions, constraints, etc. on this table, but not the table itself
                _scripter.Options = new ScriptingOptions ();
                _scripter.Options.XmlIndexes = true;
                _scripter.Options -= ScriptOption.PrimaryObject;

                TableScriptOptions xmlOptions = options;
                if ((options & TableScriptOptions.PrimaryXmlIndexes) != 0)
                    xmlOptions &= ~TableScriptOptions.AddAtEnd;
                DropAndReAdd (tree.FirstChild.Urn, addObjects, xmlOptions);
            }

            // drop the objects on the table itself
            if ((options & TableScriptOptions.IncludeTableModifiers) != 0)
            {
                options &= ~TableScriptOptions.ScriptAnonymousConstraints;

                // drop all of the permissions, constraints, etc. on this table, but not the table itself
                _scripter.Options = new ScriptingOptions ();
                _scripter.Options.DriAll = true;
				_scripter.Options.NonClusteredIndexes = true;
				_scripter.Options.ClusteredIndexes = true;
                _scripter.Options -= ScriptOption.PrimaryObject;
                DropAndReAdd (tree.FirstChild.Urn, addObjects, options);

                // script the permissions on the table
                ScriptPermissions (tree.FirstChild.Urn, addObjects);

                _scripter.Options = new ScriptingOptions (ScriptOption.Triggers);
                DropAndReAdd (tree.FirstChild.Urn, addObjects, options);
            }

			ResetConnectionToCorrectDatabase();
        }

		private void ResetConnectionToCorrectDatabase()
		{
			// SMO changes databases, so switch back here
			if (_connection.Database != _databaseName)
				_connection.ChangeDatabase(_databaseName);
		}

		private void DropViewDependencies (Urn urn, List<SchemaObject> addObjects)
		{
			DependencyTree tree = _scripter.DiscoverDependencies (new Urn [] { urn }, DependencyType.Children);
			for (DependencyTreeNode dependent = tree.FirstChild.FirstChild; dependent != null; dependent = dependent.NextSibling)
			{
				// for each child object, script it and its permissions
				_scripter.Options = new ScriptingOptions ();
				_scripter.Options.DriAll = true;
				_scripter.Options.Permissions = true;
				DropAndReAdd (dependent.Urn, addObjects, TableScriptOptions.AddAtEnd);
			}

			ResetConnectionToCorrectDatabase();
		}

		private void DropViewDependencies (string viewName, List<SchemaObject> addObjects)
		{
			View view = _database.Views [SchemaObject.UnformatSqlName (viewName)];
			if (view == null)
				return;

			DropViewDependencies (view.Urn, addObjects);
		}

		private void DropTypeDependencies (string name, List<SchemaObject> addObjects)
		{
			var type = _database.UserDefinedTableTypes[SchemaObject.UnformatSqlName(name)];
			if (type == null)
				return;

			DropTypeDependencies(type.Urn, addObjects);
		}

		private void DropTypeDependencies (Urn urn, List<SchemaObject> addObjects)
		{
			DependencyTree tree = _scripter.DiscoverDependencies(new Urn[] { urn }, DependencyType.Children);
			for (DependencyTreeNode dependent = tree.FirstChild.FirstChild; dependent != null; dependent = dependent.NextSibling)
			{
				// for each child object, script it and its permissions
				_scripter.Options = new ScriptingOptions();
				_scripter.Options.DriAll = true;
				_scripter.Options.Permissions = true;
				DropAndReAdd(dependent.Urn, addObjects, TableScriptOptions.AddAtEnd);
			}
		}

		/// <summary>
        /// Script the permissions on an object and save the script to add the permissions back later
        /// </summary>
        /// <param name="urn">The object to drop</param>
        /// <param name="addObjects">The list of addObjects, or null to not re-add dependencies</param>
		private void ScriptPermissions (Urn urn, List<SchemaObject> addObjects)
        {
            if (addObjects == null)
                return;

            // generate the script and add it if we've generated anything
            _scripter.Options = new ScriptingOptions (ScriptOption.Permissions);
            _scripter.Options.IncludeIfNotExists = true;
            _scripter.Options.ScriptDrops = false;
            string addScript = GenerateScript (urn);

            // scripting permissions on a function returns the body
			if (addScript.IndexOf("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase) >= 0)
                addScript = "";

            if (addScript.Length > 0)
                addObjects.Add (new SchemaObject (SchemaObjectType.Permission, "Scripted Permissions", addScript));

			ResetConnectionToCorrectDatabase();
		}

        /// <summary>
        /// Drop an object and generate the script to re-add it
        /// </summary>
        /// <param name="urn">The object to drop</param>
        /// <param name="addObjects">The list of addObjects, or null to not re-add dependencies</param>
        /// <param name="options">Options for scripting</param>
        private void DropAndReAdd (Urn urn, List<SchemaObject> addObjects, TableScriptOptions options)
        {
            // generate the script to readd
            if (addObjects != null)
            {
                _scripter.Options.ScriptDrops = false;
                _scripter.Options.IncludeIfNotExists = true;
                string addScript = GenerateScript (urn);

                // unnamed primary keys and constraints need to not be auto-added, since they must be built into the table
				if ((options & TableScriptOptions.ScriptAnonymousConstraints) != 0)
					addScript = _anonymousReferenceRegex.Replace (addScript, "IF 0=1 $0");
				else
				{
					addScript = _anonymousRegex.Replace (addScript, "IF 0=1 $0");
					addScript = _anonymousDefaultRegex.Replace (addScript, "IF 0=1 $0");
				}

                // if the database has autostatistics, then skip all statistics
                addScript = _statisticsRegex.Replace (addScript, "");

                // create triggers must be the first statement in the batch
				int pos = addScript.IndexOf("CREATE TRIGGER", StringComparison.OrdinalIgnoreCase);
                if (pos >= 0)
                    addScript = addScript.Substring (pos);

                // remove primary xml indexes if we don't need them
                if ((options & TableScriptOptions.PrimaryXmlIndexes) == 0)
                    addScript = _primaryXmlIndex.Replace (addScript, "IF 0=1 $0");

                if (addScript.Length > 0)
                {
                    SchemaObject newObject = new SchemaObject (SchemaObjectType.Script, "Scripted Dependencies", addScript);
                    if ((options & TableScriptOptions.AddAtEnd) != 0)
                        addObjects.Add (newObject);
                    else
                        addObjects.Insert (0, newObject);
                }
            }

            // script the drop of everything
            _scripter.Options.ScriptDrops = true;
			_scripter.Options.IncludeIfNotExists = true;
            string dropScript = GenerateScript (urn);

            // the scripter should not be scripting the table drop, so we have to comment it out
            // note that the !PrimaryObject option above works for the create script
            SqlSmoObject smo = _scripter.Server.GetSmoObject (urn);
            Table table = smo as Table;
            if (table != null)
                dropScript = _dropTableRegex.Replace (dropScript, "SELECT 1");


			ResetConnectionToCorrectDatabase();

			if (!String.IsNullOrWhiteSpace(dropScript))
	            ExecuteNonQuery (dropScript);

			ResetScripter ();
        }

        /// <summary>
        /// Matches a DROP TABLE statement
        /// </summary>
        private static readonly Regex _dropTableRegex = new Regex (String.Format (CultureInfo.InvariantCulture, @"DROP \s+ TABLE \s+ {0}", SchemaObject.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Matches a CREATE STATISTICS statement
        /// </summary>
        private static readonly Regex _statisticsRegex = new Regex (String.Format (CultureInfo.InvariantCulture, @"CREATE\s+STATISTICS\s+(?<name>{0})\s+ON\s+{0}\({0}(,\s*{0})*\)", SchemaObject.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Matches anonymous keys and check constraints
        /// </summary>
        private static readonly Regex _anonymousRegex = new Regex (String.Format (CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+{0}(\s+WITH\s+CHECK)?\s+ADD\s+(((PRIMARY|FOREIGN)\s+KEY(\s+(NON)?CLUSTERED)?)|(CHECK))", SchemaObject.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Matches anonymous keys and check constraints on table references.
        /// </summary>
        /// <remarks>We need to script anonymous FKs on reference tables, because we drop them</remarks>
        private static readonly Regex _anonymousReferenceRegex = new Regex (String.Format (CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+{0}(\s+WITH\s+CHECK)?\s+ADD\s+(((PRIMARY)\s+KEY(\s+(NON)?CLUSTERED)?)|(CHECK))", SchemaObject.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Matches anonymous keys and check constraints on table references.
        /// </summary>
        /// <remarks>We need to script anonymous FKs on reference tables, because we drop them</remarks>
        private static readonly Regex _anonymousDefaultRegex = new Regex (String.Format (CultureInfo.InvariantCulture, @"ALTER\s+TABLE\s+{0}\s+ADD\s+DEFAULT\s+\([^\]]+]", SchemaObject.SqlNameExpression), RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Matches primary xml index 
        /// </summary>
        private static readonly Regex _primaryXmlIndex = new Regex (@"CREATE\s+PRIMARY\s+XML\s+INDEX", RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

        /// <summary>
        /// Generate a script for a database object.
        /// </summary>
        /// <param name="urns">The object to script</param>
        /// <returns>String containg the script</returns>
        private string GenerateScript (Urn urn)
        {
            UrnCollection urns = new UrnCollection ();
            urns.Add (urn);

            return GenerateScript (urns);
        }

        /// <summary>
        /// Generate a script for a database object.
        /// </summary>
        /// <param name="urns">The objects to script</param>
        /// <returns>String containg the script</returns>
        private string GenerateScript (UrnCollection urns)
        {
            _scripter.Options.ContinueScriptingOnError = true;

            // script the the list of objects passed in
            StringCollection scriptCollection = _scripter.Script (urns);
            StringBuilder sb = new StringBuilder ();
            foreach (string s in scriptCollection)
                sb.AppendLine (s);

            return sb.ToString();
        }
        #endregion

        #region Private Members
        /// <summary>
        /// Opens the connection to the database and get a new command
        /// </summary>
        /// <returns>The connection to the database</returns>
        private SqlConnection OpenConnection ()
        {
            // connect to the database
            _connection = new SqlConnection (_connectionString);
            _connection.Open ();
            _command = new SqlCommand ();
            _command.Connection = _connection;

			ResetScripter ();

            return _connection;
        }

		/// <summary>
		/// Reset the scripter so the objects aren't cached
		/// </summary>
		private void ResetScripter ()
		{
            // prepare the SMO objects
            ServerConnection connection = new ServerConnection (_connection);
            Server server = new Server (connection);
            _database = server.Databases.OfType<Microsoft.SqlServer.Management.Smo.Database>().FirstOrDefault(d => String.CompareOrdinal(d.Name, _databaseName) == 0);
            _scripter = new Scripter (server);
		}

        /// <summary>
        /// Execute sql directly
        /// </summary>
        /// <param name="sql">The sql to execute</param>
        internal void ExecuteNonQuery (string sql)
        {
			// append the contents of the command
			if (_scripts != null)
			{
				_scripts.AppendLine (sql);
				_scripts.AppendLine ("GO");
			}

			// never time out an upgrade
			_command.CommandTimeout = 0;
            _command.CommandText = sql;
            _command.ExecuteNonQuery ();

			ResetScripter ();
        }

		/// <summary>
		/// Execute sql directly
		/// </summary>
		/// <param name="sql">The sql to execute</param>
		void IDbInstallConnection.ExecuteNonQuery(string sql)
		{
			ExecuteNonQuery(sql);
		}

		/// <summary>
		/// Execute sql directly
		/// </summary>
		/// <param name="sql">The sql to execute</param>
		IDataReader IDbInstallConnection.GetDataReader(string sql)
		{
			// never time out an upgrade
			_command.CommandTimeout = 0;
			_command.CommandText = sql;
			return _command.ExecuteReader();
		}

        /// <summary>
        /// Check to see if the database exists
        /// </summary>
        /// <returns>True if the database already exists</returns>
        private bool DatabaseExists ()
        {
            _command = new SqlCommand ("SELECT COUNT (*) FROM master..sysdatabases WHERE name = @DatabaseName", _connection);
            _command.Parameters.AddWithValue ("@DatabaseName", _databaseName);
            int count = (int)_command.ExecuteScalar ();
            _command.Parameters.Clear ();

            return count > 0;
        }

        /// <summary>
        /// The connection string to the database
        /// </summary>
        private string _connectionString;

		/// <summary>
		/// The connection string to the master database (for create/drop)
		/// </summary>
		private string _masterConnectionString;

		/// <summary>
        /// The name of the database to edit
        /// </summary>
        private string _databaseName;

        /// <summary>
        /// The command to use to connect to the database
        /// </summary>
        private SqlCommand _command;

        /// <summary>
        /// The current connection to the database
        /// </summary>
        private SqlConnection _connection;

        /// <summary>
        /// The SMO object for scripting
        /// </summary>
        private Scripter _scripter;
        
        /// <summary>
        /// The SMO object for connecting to the databas
        /// </summary>
        private Microsoft.SqlServer.Management.Smo.Database _database;
        #endregion

        #region Security Methods
        /// <summary>
        /// Makes sure that a name of a schema object does not contain any insecure characters.
        /// </summary>
        /// <param name="name">The name to check</param>
        /// <exception cref="ArgumentException">If a parameter contains an invalid SQL character</exception>
        /// <exception cref="ArgumentNullException">If the name is null</exception>
		private static void AssertValidSqlName(string name)
        {
            if (name == null)
                throw new ArgumentNullException ("name");
            if (name.Length == 0 || name.IndexOfAny (_insecureSqlChars) >= 0)
                throw new ArgumentException (String.Format (CultureInfo.CurrentCulture, Resources.InvalidSqlObjectName, name));
        }

        /// <summary>
        /// Characters that could cause bad sql things to happen
        /// </summary>
        /// <remarks>
        ///     -   can create a comment
        ///     ;   can end a statement
        ///     '   can end a string
        ///     "   can end a string
        /// </remarks>
        private static readonly char[] _insecureSqlChars = new char[] { '-', ';', '\'' };
        #endregion

		#region Import Method
		/// <summary>
		/// Imports an existing database into the schema registry
		/// </summary>
		/// <param name="schemaGroup">The name of the schema group to script to</param>
		public void Import (string schemaGroup)
		{
			using (TransactionScope transaction = new TransactionScope (TransactionScopeOption.Required, new TimeSpan ()))
			{
				// open the connection
				OpenConnection ();

				// make sure we have a schema registry
				SchemaRegistry registry = new SchemaRegistry (_connection);

				// get all of the objects in the current database
				_command.CommandText = @"
					SELECT o.name, o.type, p.name
						FROM sys.objects o
						LEFT JOIN sys.objects p ON (o.parent_object_id = p.object_id)
						LEFT JOIN sys.default_constraints df ON (o.object_id = df.object_id)
						WHERE o.is_ms_shipped = 0 
							-- don't import anonymous defaults
							AND (df.is_system_named IS NULL OR df.is_system_named = 0)
							AND o.Name NOT LIKE '%Insight_SchemaRegistry%'
					UNION
					select i.name, 'IX', o.name
						FROM sys.indexes i
						JOIN sys.objects o ON (i.object_id = o.object_id)
						WHERE o.is_ms_shipped = 0 AND i.type_desc <> 'HEAP' and is_primary_key = 0 and is_unique_constraint = 0";
				using (SqlDataReader reader = _command.ExecuteReader ())
				{
					while (reader.Read ())
					{
						SchemaObjectType type;

						string name = String.Format(CultureInfo.InvariantCulture, "[{0}]", reader.GetString(0));
						string sqlType = reader.GetString (1);

						switch (sqlType.Trim())
						{
							case "U":
								type = SchemaObjectType.Table;
								break;

							case "P":
								type = SchemaObjectType.StoredProcedure;
								break;

							case "V":
								type = SchemaObjectType.View;
								break;

							case "FN":
							case "TF":
								type = SchemaObjectType.Function;
								break;

							case "D":
							case "UQ":
							case "C":
								type = SchemaObjectType.Constraint;
								name = String.Format(CultureInfo.InvariantCulture, "[{0}].[{1}]", reader.GetString(2), reader.GetString(0));
								break;

							case "PK":
								type = SchemaObjectType.PrimaryKey;
								name = String.Format(CultureInfo.InvariantCulture, "[{0}].[{1}]", reader.GetString(2), reader.GetString(0));
								break;

							case "F":
								type = SchemaObjectType.ForeignKey;
								name = String.Format(CultureInfo.InvariantCulture, "[{0}].[{1}]", reader.GetString(2), reader.GetString(0));
								break;

							case "IX":
								type = SchemaObjectType.Index;
								name = String.Format(CultureInfo.InvariantCulture, "[{0}].[{1}]", reader.GetString(2), reader.GetString(0));
								break;

							case "SQ":
								// query notification, skip
								continue;

							default:
								throw new InvalidOperationException(String.Format(CultureInfo.InvariantCulture, "Cannot import object {0} of type {1}", name, sqlType));
						}

						SchemaObject schemaObject = new SchemaObject (type, name, "");
						registry.UpdateObject(schemaObject, schemaGroup, this, null);
					}
				}

				registry.Update ();

				transaction.Complete ();
			}
		}
		#endregion

		#region Event Handling
		/// <summary>
        /// Called when a table is being converted.
        /// </summary>
        /// <remarks>
        ///     When the event is fired, the table data exists in the before table.
        ///     If the event handler converts the data, return true.
        ///     Return false to allow the installer to use the default conversion.
        /// </remarks>
        public event EventHandler<ConvertTableEventArgs> ConvertingTable;

        /// <summary>
        /// Called before a table is updated
        /// </summary>
        public event EventHandler<SchemaEventArgs> UpdatingTable;

        /// <summary>
        /// Called after a table is updated
        /// </summary>
        public event EventHandler<SchemaEventArgs> UpdatedTable;

        /// <summary>
        /// Called before a SchemaObject is created
        /// </summary>
        public event EventHandler<SchemaEventArgs> CreatingObject;

        /// <summary>
        /// Called after a SchemaObject is created
        /// </summary>
        public event EventHandler<SchemaEventArgs> CreatedObject;

        /// <summary>
        /// Called before a SchemaObject is dropped
        /// </summary>
        public event EventHandler<SchemaEventArgs> DroppingObject;

		/// <summary>
		/// Called when an object is missing
		/// </summary>
		public event EventHandler<SchemaEventArgs> MissingObject;
		internal void OnMissingObject (object sender, SchemaEventArgs e)
		{
			if (MissingObject != null)
				MissingObject (sender, e);
		}
        #endregion

		public void Dispose()
		{
			if (_command != null)
			{
				_command.Dispose();
				_command = null;
			}
			if (_connection != null)
			{
				_connection.Dispose();
				_connection = null;
			}
		}
	}
    #endregion

	/// <summary>
	/// Determines how aggressively to rebuild the database
	/// </summary>
	public enum RebuildMode
	{
		/// <summary>
		/// Only build if there are changes
		/// </summary>
		DetectChanges,

		/// <summary>
		/// Rebuild anything that can be safely updated without taking a long time
		/// </summary>
		RebuildSafe,

		/// <summary>
		/// Rebuild anything that can be rebuilt
		/// </summary>
		RebuildFull
	}
}
