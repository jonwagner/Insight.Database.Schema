using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Insight.Database.Schema.Tests
{
	[TestFixture]
	public class SqlParserTests
	{
		/// <summary>
		/// Make sure that all forms of SQL names are detected properly.
		/// </summary>
		/// <param name="name"></param>
		[Test]
		public void TestSqlNameParsing([Values (
			"type::[AccountsTableType]",
			"type::[dbo].[AccountsTableType]",
			"dbo.Accounts",
			"Accounts",
			"foo.dbo.Accounts",
			"[Foo].dbo.Accounts"
		)] string name)
		{
			Regex regex = new Regex(SqlParser.SqlNameExpression, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.ExplicitCapture | RegexOptions.Compiled);

			Match m = regex.Match(name);
			Assert.IsTrue(m.Success);
			Assert.AreEqual(m.Value, name);
		}

        /// <summary>
        /// Make sure that we throw exceptions for all of the SQL we don't support.
        /// </summary>
        /// <param name="sql">The sql to check.</param>
        [Test]
        public void TestUnsupportedSql([Values(
            "ALTER TABLE Foo ADD DEFAULT (0) FOR Col",          // unnamed defaults
            "CREATE TABLE Foo (id int DEFAULT (0))",            // inline unnamed defaults
            "ALTER TABLE Foo ADD CONSTRAINT CHECK (id > 0)",    // unnamed check constraints
            "CREATE TABLE Foo (id int, CHECK (id > 0))",        // inline unnamed check constraints
            "ALTER TABLE Foo ADD PRIMARY KEY (ID)",             // unnamed primary key
            "CREATE TABLE Foo (id int, PRIMARY KEY (ID))",      // inline unnamed primary key
            "ALTER TABLE Foo ADD FOREIGN KEY (ID)",             // unnamed foreign key
            "CREATE TABLE Foo (id int, FOREIGN KEY (ID))",      // inline unnamed foreign key
            ""
        )] string sql)
        {
            if (sql == "") return;

            Assert.Throws<SchemaParsingException>(() => new SchemaObject(sql));
        }
	}
}
