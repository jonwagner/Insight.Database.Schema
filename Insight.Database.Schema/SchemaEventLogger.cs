#region Using directives

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using Insight.Database.Schema.Properties;
#endregion

namespace Insight.Database.Schema
{
    #region SchemaEventLogger Class
    /// <summary>
    /// Logs events from a schema installer.
    /// </summary>
    public abstract class SchemaEventLogger
    {
        #region Constructors
        /// <summary>
        /// Construct an event logger
        /// </summary>
        protected SchemaEventLogger ()
        {
            _onSchemaChange = new EventHandler<SchemaEventArgs> (OnSchemaChange);
        }

        /// <summary>
        /// Construct an event logger and attach it to a schema installer.
        /// </summary>
        /// <param name="installer">The installer to monitor</param>
        protected SchemaEventLogger (SchemaInstaller installer) : this()
        {
            Attach (installer);
        }
        #endregion

        #region Attach/Detach Methods
        /// <summary>
        /// Attach to the events of a SchemaInstaller
        /// </summary>
        /// <param name="installer">The installer to attach to</param>
        public void Attach (SchemaInstaller installer)
        {
            installer.DroppingObject += _onSchemaChange;
            installer.CreatingObject += _onSchemaChange;
            installer.CreatedObject += _onSchemaChange;
        }

        /// <summary>
        /// Detach from the events of a SchemaInstaller
        /// </summary>
        /// <param name="installer">The installer to detach from</param>
        public void Detach (SchemaInstaller installer)
        {
            installer.DroppingObject -= _onSchemaChange;
            installer.CreatingObject -= _onSchemaChange;
            installer.CreatedObject -= _onSchemaChange;
        }

        private EventHandler<SchemaEventArgs> _onSchemaChange;
        #endregion

        #region Event Handlers
        /// <summary>
        /// Called when a schema event is received
        /// </summary>
        /// <param name="se">The schema event</param>
        public abstract void LogSchemaChange (SchemaEventArgs se);

        private void OnSchemaChange (object sender, SchemaEventArgs se)
        {
            LogSchemaChange (se);
        }
        #endregion

        #region Support Methods
        /// <summary>
        /// Format a schema event into a string
        /// </summary>
        /// <param name="se">The schema event to format</param>
        /// <returns>The formatted event</returns>
        protected static string FormatEventArgs (SchemaEventArgs se)
        {
            string eventName;
            switch (se.EventType)
            {
                case SchemaEventType.BeforeDrop:
                    eventName = Resources.Dropping;
                    break;
                case SchemaEventType.BeforeCreate:
                    eventName = Resources.Creating;
                    break;
                case SchemaEventType.AfterCreate:
                    eventName = Resources.Created;
                    break;
                default:
                    eventName = Enum.Format (typeof (SchemaEventType), se.EventType, "G");
                    break;
            }

            return String.Format (CultureInfo.CurrentCulture, "{0} {1}", eventName, se.ObjectName);
        }
        #endregion
    }
    #endregion
}
