#region Using directives

using System;
using System.Collections.Generic;
using System.Text;

#endregion

namespace Insight.Database.Schema
{
    /// <summary>
    /// The type of schema event
    /// </summary>
    public enum SchemaEventType
    {
        BeforeDrop,
        BeforeTableUpdate,
        AfterTableUpdate,
        BeforeCreate,
        AfterCreate,
        ConvertTable,
		MissingObject
    }
}
