using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;

namespace Insight.Database.Schema.Tests
{
	[TestFixture]
    public class SingularizerTests
	{
		private void Test(string plural, string singular)
		{
			Assert.AreEqual(plural.ToLower(), Singularizer.Singularize(singular).ToLower());
		}

		#region Singular Tests
		[Test]
		public void TestSinglePlural()
		{
			Test("person", "people");
			Test("octopus", "octopi");
			Test("deer", "deer");
			Test("wolf", "wolves");
			Test("wife", "wives");
			Test("compass", "compasses");
			Test("cliff", "cliffs");
			Test("turf", "turfs");
			Test("case", "cases");
			Test("objective", "objectives");
			Test("size", "sizes");
		}
		#endregion
	}
}
