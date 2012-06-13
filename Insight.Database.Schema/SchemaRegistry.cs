#region Using directives

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Text;

#endregion

namespace Insight.Database.Schema
{
    /// <summary>
    /// Manages the list of schema objects installed in the database.
    /// </summary>
    class SchemaRegistry : IDisposable
    {
        #region Constructors
        /// <summary>
        /// Manages the signatures of objects in the schema database
        /// </summary>
        /// <param name="connection">The connection to the database</param>
        public SchemaRegistry (SqlConnection connection)
        {
            _connection = connection;
            CreateTable ();
            Load ();
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// Determine if the database already contains a given object
        /// </summary>
        /// <param name="schemaObject">The schema object to look for</param>
        /// <returns>True if the object is in the registry, false otherwise</returns>
        public bool Contains (string objectName)
        {
            string select = String.Format (CultureInfo.InvariantCulture, "ObjectName = '{0}'", objectName);
            if (RegistryTable.Select (select).Length > 0)
                return true;
            else
                return false;
        }

        /// <summary>
        /// Delete a schema object from the registry
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        public void DeleteObject (string objectName)
        {
            DataRow row = FindRow (objectName);
            if (row != null)
                row.Delete ();
        }

        /// <summary>
        /// Get the signature of an object in the database
        /// </summary>
        /// <param name="objectName">Name of the object to find</param>
        /// <returns>The signature of the object</returns>
        public string GetSignature (string objectName)
        {
            DataRow row = FindRow (objectName);
            return row["Signature"].ToString ();
        }

        /// <summary>
        /// Get the type of an object in the database
        /// </summary>
        /// <param name="objectName">Name of the object to find</param>
        /// <returns>The type of the object</returns>
        public SchemaObjectType GetObjectType (string objectName)
        {
            DataRow row = FindRow (objectName);
            return (SchemaObjectType)Enum.Parse (typeof (SchemaObjectType), row["Type"].ToString ());
        }

		/// <summary>
		/// Get the original order of an object in the database
		/// </summary>
		/// <param name="objectName">Name of the object to find</param>
		/// <returns>The type of the object</returns>
		public int GetOriginalOrder(string objectName)
		{
			DataRow row = FindRow(objectName);
			object o = row["OriginalOrder"];
			if (o == DBNull.Value)
				return 0;
			return Convert.ToInt32(o, CultureInfo.InvariantCulture);
		}

        /// <summary>
        /// Add or update an object in the schema registry
        /// </summary>
        /// <param name="schemaObject">The object to update</param>
        /// <param name="schemaGroup">The name of the schema group</param>
        public void UpdateObject (SchemaObject schemaObject, string schemaGroup, SchemaInstaller installer, IEnumerable<SchemaObject> objects)
        {
            DeleteObject (schemaObject.Name);
            RegistryTable.Rows.Add (new object[] { schemaGroup, schemaObject.Name, schemaObject.GetSignature(installer, objects), schemaObject.SchemaObjectType.ToString (), schemaObject.OriginalOrder });
        }

        /// <summary>
        /// Returns a list of objects in the given schema group
        /// </summary>
        /// <param name="schemaGroup">The name of the group to return</param>
        /// <returns>List of object names</returns>
        public List<string> GetObjectNames (string schemaGroup)
        {
            string select = String.Format (CultureInfo.InvariantCulture, "SchemaGroup = '{0}'", schemaGroup);

            // go through the data list and return all of the items in the group
            List<string> list = new List<string> ();
            foreach (DataRow row in RegistryTable.Select (select))
            {
                list.Add (row["ObjectName"].ToString());
            }

            return list;
        }

        /// <summary>
        /// Update the changed registry data
        /// </summary>
        public void Update ()
        {
            _adapter.Update (_registry);
        }
        #endregion

        #region Private Methods
        /// <summary>
        /// Create the schema registry table in the database
        /// </summary>
        private void CreateTable()
        {
            // see if the schema registry table already exists
            SqlCommand command = new SqlCommand ();
            command.Connection = _connection;
            command.CommandText = "SELECT COUNT (*) FROM sysobjects WHERE name = @TableName";
            command.Parameters.AddWithValue ("@TableName", _schemaRegistryTableName);
            int count = (int)command.ExecuteScalar ();
			if (count == 0)
			{
				// create the schema registry table
				command.CommandText = String.Format (CultureInfo.InvariantCulture, @"
					CREATE TABLE [{0}]
					(
						[SchemaGroup] [varchar](64) NOT NULL,
						[ObjectName] [varchar](256) NOT NULL,
						[Signature] [varchar](28) NOT NULL,
						[Type][varchar](32) NOT NULL,
						[OriginalOrder] [int] DEFAULT (0)
						CONSTRAINT PK_{0} PRIMARY KEY ([ObjectName])
					)
		           ", _schemaRegistryTableName);
			}
			else
			{
				// add in the new columns
				command.CommandText = String.Format(CultureInfo.InvariantCulture, @"
					IF NOT EXISTS (SELECT * FROM sys.syscolumns WHERE id = OBJECT_ID ('insight_schemaregistry') AND name = 'OriginalOrder') 
						ALTER TABLE {0} ADD OriginalOrder [int]
					", _schemaRegistryTableName); 
			}

			command.Parameters.Clear ();
	        command.ExecuteNonQuery ();
        }

        /// <summary>
        /// Load the registry data from the database
        /// </summary>
        private void Load ()
        {
            _adapter = new SqlDataAdapter (String.Format (CultureInfo.InvariantCulture, "SELECT * FROM {0}", _schemaRegistryTableName), _connection);
            _registry = new DataSet ();
            _registry.Locale = CultureInfo.InvariantCulture;
            _adapter.Fill (_registry);

			// use the command builder to fill in the other commands
			// this seems to be necessary for some environments
			SqlCommandBuilder builder = new SqlCommandBuilder(_adapter);
			_adapter.InsertCommand = builder.GetInsertCommand();
			_adapter.UpdateCommand = builder.GetUpdateCommand();
			_adapter.DeleteCommand = builder.GetDeleteCommand();

			RegistryTable.PrimaryKey = new DataColumn [] { RegistryTable.Columns ["ObjectName"] };
        }

        /// <summary>
        /// Find a row containing an object
        /// </summary>
        /// <param name="objectName">The name of the object to find</param>
        /// <returns>The row containing the object</returns>
        private DataRow FindRow (string objectName)
        {
            return RegistryTable.Rows.Find (new object[] { objectName });
        }
        #endregion

        #region Private Data
        /// <summary>
        /// The name of the schema registry table
        /// </summary>
        private const string _schemaRegistryTableName = "Insight_SchemaRegistry";

        /// <summary>
        /// The connection
        /// </summary>
        private SqlConnection _connection;

        /// <summary>
        /// The data adapter for synchronizing the registry data
        /// </summary>
        private SqlDataAdapter _adapter;

        /// <summary>
        /// The data table in memory
        /// </summary>
        /// <value>The DataTable containing the registry records</value>
        private DataTable RegistryTable { get { return _registry.Tables[0]; } }

        /// <summary>
        /// The schema registry
        /// </summary>
        private DataSet _registry;
        #endregion

		public void Dispose()
		{
			if (_registry != null)
			{
				_registry.Dispose();
				_registry = null;
			}
			if (_registry != null)
			{
				_adapter.Dispose();
				_adapter = null;
			}
		}
	}
}
