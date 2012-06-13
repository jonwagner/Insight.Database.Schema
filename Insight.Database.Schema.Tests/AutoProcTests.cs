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
    public class AutoProcTests
	{
		#region Signature Tests
		[Test]
		public void AutoProcSignatureShouldChangeEachTimeIfNoContextIsGiven()
		{
			AutoProc p1 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, null);
			AutoProc p2 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, null);

			Assert.AreNotEqual(p1.Signature, p2.Signature);
		}

		[Test]
		public void AutoProcSignatureShouldNotChangeIfContextIsGiven()
		{
			var o1 = new List<SchemaObject>() 
			{
				new SchemaObject("CREATE TABLE [Beer] ([id] int)")
			};
			AutoProc p1 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, o1);
			AutoProc p2 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, o1);

			Assert.AreEqual(p1.Signature, p2.Signature);
		}

		[Test]
		public void AutoProcSignatureShouldChangeIfTableChanges()
		{
			var o1 = new List<SchemaObject>() 
			{
				new SchemaObject("CREATE TABLE [Beer] ([id] int)")
			};
			AutoProc p1 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, o1);

			var o2 = new List<SchemaObject>() 
			{
				new SchemaObject("CREATE TABLE [Beer] ([id] int IDENTITY)")
			};
			AutoProc p2 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, o2);

			Assert.AreNotEqual(p1.Signature, p2.Signature);
		}

		[Test]
		public void AutoProcSignatureShouldChangeIfPrimaryKeyChanges()
		{
			var o1 = new List<SchemaObject>() 
			{
				new SchemaObject("ALTER TABLE [Beer] WITH NOCHECK ADD CONSTRAINT [PK_Beer] PRIMARY KEY NONCLUSTERED ([ID])")
			};
			AutoProc p1 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, o1);

			var o2 = new List<SchemaObject>() 
			{
				new SchemaObject("ALTER TABLE [Beer] WITH NOCHECK ADD CONSTRAINT [PK_Beer] PRIMARY KEY NONCLUSTERED ([ID], [Name])")
			};
			AutoProc p2 = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", null, o2);

			Assert.AreNotEqual(p1.Signature, p2.Signature);
		}
		#endregion

		#region Default Name Tests
		[Test]
		public void AutoProcShouldGenerateNamesAutomatically()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC Insert [Beer]", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [InsertBeer] (@ID int, @Name varchar(256), @OriginalGravity decimal(18,2)) AS INSERT INTO [Beer] (ID, Name, OriginalGravity) OUTPUT  VALUES (@ID, @Name, @OriginalGravity) GO ", p.Sql);
		}
		#endregion

		#region Name Tests
		[Test]
		public void AutoProcAllowsForTemplateNames()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Insert [Users] {1}_{0}", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [Users_Insert] (@ID int) AS INSERT INTO [Users] (ID) OUTPUT  VALUES (@ID) GO ", p.Sql);
		}
		#endregion

		#region Query Generation Tests
		[Test]
		public void TestAllGeneration()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC All [Beer]", columns.Object, null);
			Assert.AreEqual("CREATE PROCEDURE [SelectBeer] (@ID int) AS SELECT * FROM [Beer] WHERE ID=@ID GO CREATE PROCEDURE [InsertBeer] (@ID int, @Name varchar(256), @OriginalGravity decimal(18,2)) AS INSERT INTO [Beer] (ID, Name, OriginalGravity) OUTPUT  VALUES (@ID, @Name, @OriginalGravity) GO CREATE PROCEDURE [UpdateBeer] (@ID int, @Name varchar(256), @OriginalGravity decimal(18,2)) AS UPDATE [Beer] SET Name=@Name, OriginalGravity=@OriginalGravity WHERE ID=@ID GO CREATE PROCEDURE [DeleteBeer] (@ID int) AS DELETE FROM [Beer] WHERE ID=@ID GO ", p.Sql);
		}

		[Test]
		public void TestSelectGeneration()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC Select [Beer] SelectBeer", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [SelectBeer] (@ID int) AS SELECT * FROM [Beer] WHERE ID=@ID GO ", p.Sql);
		}

		[Test]
		public void TestInsertGeneration()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [InsertBeer] (@ID int, @Name varchar(256), @OriginalGravity decimal(18,2)) AS INSERT INTO [Beer] (ID, Name, OriginalGravity) OUTPUT  VALUES (@ID, @Name, @OriginalGravity) GO ", p.Sql);
		}

		[Test]
		public void TestUpdateGeneration()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC Update [Beer] UpdateBeer", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [UpdateBeer] (@ID int, @Name varchar(256), @OriginalGravity decimal(18,2)) AS UPDATE [Beer] SET Name=@Name, OriginalGravity=@OriginalGravity WHERE ID=@ID GO ", p.Sql);
		}

		[Test]
		public void TestDeleteGeneration()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC Delete [Beer] DeleteBeer", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [DeleteBeer] (@ID int) AS DELETE FROM [Beer] WHERE ID=@ID GO ", p.Sql);
		}
		#endregion

		#region Singular Tests
		[Test]
		public void TestPeopleToPerson()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Delete [People]", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [DeletePerson] (@ID int) AS DELETE FROM [People] WHERE ID=@ID GO ", p.Sql);
		}

		[Test]
		public void TestOctopiToOctopus()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Delete [Octopi]", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [DeleteOctopus] (@ID int) AS DELETE FROM [Octopi] WHERE ID=@ID GO ", p.Sql);
		}

		[Test]
		public void TestDeerToDeer()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Delete [Deer]", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [DeleteDeer] (@ID int) AS DELETE FROM [Deer] WHERE ID=@ID GO ", p.Sql);
		}
		#endregion
	}
}
