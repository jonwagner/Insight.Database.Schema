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
	public class SchemaParsingException : Exception
    {
        /// <summary>
        /// Construct a SchemaParsingException
        /// </summary>
        public SchemaParsingException ()
        {
        }

        /// <summary>
        /// Construct a SchemaParsingException with a message
        /// </summary>
        /// <param name="message">The exception error message</param>
        public SchemaParsingException (string message) : base (message)
        {
        }

        /// <summary>
        /// Construct a SchemaParsingException with a message
        /// </summary>
        /// <param name="message">The exception error message</param>
        /// <param name="innerException">The base exception</param>
        public SchemaParsingException (string message, Exception innerException) : base (message, innerException)
        {
        }

        /// <summary>
        /// Construct a SchemaParsingException with a message and the error sql
        /// </summary>
        /// <param name="message">The exception error message</param>
        /// <param name="sql">The sql script that could not be parsed</param>
		public SchemaParsingException(string message, string sql)
			: base(String.Format(CultureInfo.InvariantCulture, message, sql))
        {
            _sql = sql;
        }

        /// <summary>
        /// Construct a SchemaParsingException with a message and the error sql
        /// </summary>
        /// <param name="info">Serialization information</param>
        /// <param name="context">Serialization context</param>
        protected SchemaParsingException (SerializationInfo info, StreamingContext context) : base (info, context)
        {
        }

        /// <summary>
        /// The SQL script that caused the error
        /// </summary>
        /// <value></value>
        public string Sql { get { return _sql; } }
        private string _sql;

		public override void GetObjectData(SerializationInfo info, StreamingContext context)
		{
			base.GetObjectData(info, context);

			info.AddValue("_sql", _sql);
		}
    }
    #endregion
}
