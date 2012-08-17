#region Using directives

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Globalization;

#endregion

namespace Insight.Database.Schema
{
    #region SchemaParsingException Class
    /// <summary>
    /// Represents errors in parsing a SQL script
    /// </summary>
	[Serializable]
	public class SchemaException : Exception
    {
        /// <summary>
        /// Construct a SchemaException
        /// </summary>
        public SchemaException ()
        {
        }

        /// <summary>
        /// Construct a SchemaException with a message
        /// </summary>
        /// <param name="message">The exception error message</param>
        public SchemaException (string message) : base (message)
        {
        }

        /// <summary>
        /// Construct a SchemaException with a message
        /// </summary>
        /// <param name="message">The exception error message</param>
        /// <param name="innerException">The base exception</param>
        public SchemaException (string message, Exception innerException) : base (message, innerException)
        {
        }

        /// <summary>
        /// Construct a SchemaException with a message and the error sql
        /// </summary>
        /// <param name="message">The exception error message</param>
        /// <param name="sql">The sql script that could not be parsed</param>
		public SchemaException(string message, string sql)
			: base(String.Format(CultureInfo.InvariantCulture, message, sql))
        {
            _sql = sql;
        }

        /// <summary>
		/// Construct a SchemaException with a message and the error sql
        /// </summary>
        /// <param name="info">Serialization information</param>
        /// <param name="context">Serialization context</param>
		protected SchemaException(SerializationInfo info, StreamingContext context)
			: base(info, context)
        {
        }

        /// <summary>
        /// The SQL script that caused the error
        /// </summary>
        /// <value></value>
        public string Sql { get { return _sql; } }
        private string _sql;

		/// <summary>
		/// Implements the serialization of the Exception data.
		/// </summary>
		/// <param name="info">The information object to append to.</param>
		/// <param name="context">The streaming context to write to.</param>
		public override void GetObjectData(SerializationInfo info, StreamingContext context)
		{
			base.GetObjectData(info, context);

			// add the SQL that was being parsed
			info.AddValue("_sql", _sql);
		}
    }
    #endregion
}
