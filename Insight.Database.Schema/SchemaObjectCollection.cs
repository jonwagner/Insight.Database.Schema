#region Using directives

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;
using System.Collections.ObjectModel;

#endregion

namespace Insight.Database.Schema
{
    #region Schema Class
    /// <summary>
    /// Contains a set of SchemaObjects
    /// </summary>
    public class SchemaObjectCollection : Collection <SchemaObject>
    {
        #region Constructors
        /// <summary>
        /// Create an empty schema for manual editing
        /// </summary>
        public SchemaObjectCollection ()
        {
        }

        /// <summary>
        /// Load a schema from a file
        /// </summary>
        /// <param name="fileName">The name of the file to load</param>
        /// <exception cref="FileNotFoundException">If the file canont be found</exception>
        /// <exception cref="ArgumentNullException">If fileName is null</exception>
        /// <exception cref="SchemaParsingException">If the schema file cannot be parsed</exception>
        public SchemaObjectCollection (string fileName)
        {
            Load (fileName);
        }

		/// <summary>
		/// Load a schema from a stream
		/// </summary>
		/// <param name="stream">The stream to load from</param>
		public SchemaObjectCollection(Stream stream)
		{
			Load(stream);
		}

		/// <summary>
		/// Load the schema from the assembly
		/// </summary>
		/// <param name="assembly"></param>
		public SchemaObjectCollection (Assembly assembly)
		{
			Load (assembly);
		}
        #endregion

		#region Properties
		/// <summary>
		/// Set to true to strip print statements on load
		/// </summary>
		public bool StripPrintStatements { get; set; }
		#endregion

		#region Load Methods
		/// <summary>
        /// Load a schema from a file
        /// </summary>
        /// <param name="fileName">The name of the file to load</param>
        /// <exception cref="FileNotFoundException">If the file canont be found</exception>
        /// <exception cref="ArgumentNullException">If fileName is null</exception>
        /// <exception cref="SchemaParsingException">If the schema file cannot be parsed</exception>
        public void Load (string fileName)
        {
            if (fileName == null) throw new ArgumentNullException ("fileName");

            using (Stream stream = new FileStream (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                Load (stream);
        }

        /// <summary>
        /// Load a schema from a stream
        /// </summary>
        /// <param name="stream">The stream to load from</param>
        /// <exception cref="ArgumentNullException">If stream is null</exception>
        /// <exception cref="SchemaParsingException">If the schema file cannot be parsed</exception>
        public void Load (Stream stream)
        {
            if (stream == null) throw new ArgumentNullException ("stream");

            using (TextReader textReader = new StreamReader (stream))
                Load (textReader);
        }

        /// <summary>
        /// Load a schema from a text reader
        /// </summary>
        /// <param name="textReader">The text reader to load from</param>
        /// <exception cref="ArgumentNullException">If textReader is null</exception>
        /// <exception cref="SchemaParsingException">If the schema file cannot be parsed</exception>
        public void Load (TextReader textReader)
        {
            StringBuilder sb = new StringBuilder ();
            
            // read in each line until we get to a GO command
            for (string line = textReader.ReadLine (); line != null; line = textReader.ReadLine ())
            {
                if (_goCommandExpression.Match (line).Success)
                {
                    // create the object, add it to the schema, and start a new schema
                    Add (sb.ToString ());
                    sb = new StringBuilder ();
                }
                else
                    sb.AppendLine (line);
            }

            // if the file doesn't end with a GO, then we need to create one more object
			string last = sb.ToString ().Trim();
			if (last.Length > 0)
				Add (last);
        }

		/// <summary>
		/// Load a schema from the resource files
		/// </summary>
		/// <returns></returns>
		public void Load (Assembly assembly)
		{
			// find all of the embedded sql in the given assembly
			foreach (string resourceName in assembly.GetManifestResourceNames ())
			{
				if (resourceName.EndsWith (".sql", StringComparison.OrdinalIgnoreCase))
				{
					string sql;

					// read in the sql
					using (Stream stream = assembly.GetManifestResourceStream (resourceName))
					using (StreamReader reader = new StreamReader (stream))
						sql = reader.ReadToEnd ();

					// now load up the sql
					using (StringReader sr = new StringReader (sql))
						Load (sr);
				}
			}
		}

        /// <summary>
        /// Add a schema object corresponding to a script
        /// </summary>
        /// <param name="sql">The sql to add</param>
        /// <exception cref="ArgumentNullException">If sql is null</exception>
        /// <exception cref="SchemaParsingException">If the sql cannot be parsed</exception>
        public void Add (string sql)
        {
			// change PRINT to --PRINT so we can keep diagnostics in the 
			if (StripPrintStatements)
				sql = sql.Replace ("PRINT", "--PRINT");

            Add (new SchemaObject (sql));
        }
        #endregion

        private static readonly Regex _goCommandExpression = new Regex (@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);
    }
    #endregion
}
