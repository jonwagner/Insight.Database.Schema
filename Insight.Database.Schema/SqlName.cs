using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Insight.Database.Schema
{
	class SqlName
	{
		public string Original { get; private set; }
		public string Schema { get; private set; }
		public string Table { get; private set; }
		public string Object { get; private set; }

		public string SchemaFormatted { get { return SqlParser.FormatSqlName(Schema); } }
		public string TableFormatted { get { return SqlParser.FormatSqlName(Table); } }
		public string ObjectFormatted { get { return SqlParser.FormatSqlName(Object); } }

		public string SchemaQualifiedTable
		{
			get
			{
				if (!String.IsNullOrWhiteSpace(Schema))
					return SchemaFormatted + "." + TableFormatted;
				else
					return TableFormatted;
			}
		}

		public string SchemaQualifiedObject
		{
			get
			{
				if (!String.IsNullOrWhiteSpace(Schema))
					return SchemaFormatted + "." + ObjectFormatted;
				else
					return ObjectFormatted;
			}
		}

		public string FullName
		{
			get
			{
				string name = SchemaQualifiedTable;
				if (Table != Object)
					name += "." + ObjectFormatted;

				return name;
			}
		}

		public SqlName(string fullName, int expectedParts)
		{
			Original = fullName;

			var split = SqlParser.SplitSqlName(fullName);
			while (split.Length < expectedParts)
			{
				fullName = "[dbo]." + fullName;
				split = SqlParser.SplitSqlName(fullName);
			}

			if (split.Length == 1)
			{
				Table = split[0];
				Object = split[0];
			}
			if (split.Length == 2)
			{
				Schema = split[0];
				Table = split[1];
				Object = split[1];
			}
			else if (split.Length == 3)
			{
				Schema = split[0];
				Table = split[1];
				Object = split[2];
			}

			if (String.IsNullOrWhiteSpace(Schema))
				Schema = "dbo";

			Schema = SqlParser.UnformatSqlName(Schema);
			Table = SqlParser.UnformatSqlName(Table);
			Object = SqlParser.UnformatSqlName(Object);
		}

		public SqlName Append(string objectName)
		{
			if (Table != Object) throw new InvalidOperationException("Cannot get a child of an object that is not a table");

			SqlName child = (SqlName)MemberwiseClone();
			child.Object = SqlParser.UnformatSqlName(objectName);

			return child;
		}
	}
}
