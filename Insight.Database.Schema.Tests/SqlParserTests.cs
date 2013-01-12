using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using NUnit.Framework;
using System.IO;

namespace Insight.Database.Schema.Tests
{
	[TestFixture]
	public class SqlParserTests
	{
		#region Parser Tests
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
            "ALTER TABLE Foo ADD CONSTRAINT CHECK (id > 0)",    // unnamed explicit check constraints
            "CREATE TABLE Foo (id int, CHECK (id > 0))",        // unnamed inline check constraints
            "ALTER TABLE Foo ADD PRIMARY KEY (ID)",             // unnamed explicit primary key
			"CREATE TABLE Foo (id int, PRIMARY KEY (ID))",      // unnamed inline primary key
            "ALTER TABLE Foo ADD FOREIGN KEY (ID)",             // unnamed explicit foreign key
			"CREATE TABLE Foo (id int, FOREIGN KEY (ID))",      // unnamed inline foreign key
            ""
        )] string sql)
        {
            if (sql == "") return;

            Assert.Throws<SchemaParsingException>(() => new SchemaObject(sql));
		}
		#endregion

		#region Schema Tests
		/// <summary>
		/// Test that when everything after the last GO is a comment or whitespace, the last section is ignored.
		/// UNLESS it's an autoproc, which can be embedded in comments.
		/// </summary>
		[Test]
		public void TestTrailingWhiteSpaceAndComments()
		{
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1");
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1 \n GO ");
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1 \n GO \n");
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1 \n GO \n \n");
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1 \n GO --comment");
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1 \n GO \n --comment");
			AssertScriptCount(1, "CREATE PROC Foo AS SELECT 1 \n GO \n --comment \n");
			AssertScriptCount(2, "CREATE PROC Foo AS SELECT 1 \n GO \n --comment \n CREATE PROC Goo AS SELECT 1 \n GO");

			// autoprocs are purely comment-based, so we need to make sure they work
			AssertScriptCount(1, "-- AUTOPROC Beer All");
			AssertScriptCount(2, "-- AUTOPROC Beer All \n GO \n -- AUTOPROC Glasses All");
			AssertScriptCount(2, "CREATE PROC Foo AS SELECT 1 \n GO \n -- AUTOPROC Beer All");
			AssertScriptCount(2, "CREATE PROC Foo AS SELECT 1 \n GO \n -- AUTOPROC Beer All");
		}

		private void AssertScriptCount(int count, string sql)
		{
			StringReader reader = new StringReader(sql);
			SchemaObjectCollection c = new SchemaObjectCollection();
			c.Load(reader);
			Assert.AreEqual(count, c.Count);
		}
		#endregion
	}
}
