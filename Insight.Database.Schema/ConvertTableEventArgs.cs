#region Using directives

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

#endregion

namespace Insight.Database.Schema
{
    /// <summary>
    /// Arguments for the OnConvertTable event
    /// </summary>
    public class ConvertTableEventArgs : SchemaEventArgs
    {
        /// <summary>
        /// The name of the table containing the pre-converted data
        /// </summary>
        /// <value>The name of the table containing the pre-converted data</value>
        public string BeforeTableName { get { return _beforeTableName; } }
        private string _beforeTableName;

        /// <summary>
        /// The name of the table to convert the data into
        /// </summary>
        /// <value>The name of the table containing the pre-converted data</value>
        public string AfterTableName { get { return _afterTableName; } }
        private string _afterTableName;

        /// <summary>
        /// The SchemaInstaller connection to the database
        /// </summary>
        /// <value>The name of the table containing the pre-converted data</value>
        public SqlConnection Connection { get { return _connection; } }
        private SqlConnection _connection;

        /// <summary>
        /// Tells the SchemaInstaller if the table data has been converted
        /// </summary>
        /// <value>The name of the table containing the pre-converted data</value>
        public bool Converted
        {
            get { return _converted; }
            set { _converted = value; }
        }
        private bool _converted;

        internal ConvertTableEventArgs (SchemaEventType eventType, SchemaObject table, SqlConnection connection, string beforeTable, string afterTable) :
            base (eventType, table)
        {
            _connection = connection;
            _beforeTableName = beforeTable;
            _afterTableName = afterTable;
        }
    }
}
