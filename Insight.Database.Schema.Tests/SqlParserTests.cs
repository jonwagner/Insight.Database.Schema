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
	}
}
