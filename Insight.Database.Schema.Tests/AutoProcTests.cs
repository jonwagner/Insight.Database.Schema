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
		private Mock<IColumnDefinitionProvider> _columns = new Mock<IColumnDefinitionProvider>();
		private IColumnDefinitionProvider Columns { get { return _columns.Object; } }

		[SetUp]
		public void SetUp()
		{
			_columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true, IsIdentity = true, IsReadOnly = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});
		}

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

			Assert.AreEqual("CREATE PROCEDURE [InsertBeer]\r\n(\r\n\t@ID int,\r\n\t@Name varchar(256),\r\n\t@OriginalGravity decimal(18,2)\r\n)\r\nAS\r\nINSERT INTO [Beer]\r\n(\r\n\t[ID],\r\n\t[Name],\r\n\t[OriginalGravity]\r\n)\r\nVALUES\r\n(\r\n\t@ID,\r\n\t@Name,\r\n\t@OriginalGravity\r\n)\r\n\r\nGO\r\n", p.Sql);
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

			AutoProc p = new AutoProc("AUTOPROC Insert [Users] Name={1}_{0}", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [Users_Insert]\r\n(\r\n\t@ID int\r\n)\r\nAS\r\nINSERT INTO [Users]\r\n(\r\n\t[ID]\r\n)\r\nVALUES\r\n(\r\n\t@ID\r\n)\r\n\r\nGO\r\n", p.Sql);
		}
		#endregion

		#region Standard CRUD Generation Tests
		[Test]
		public void TestAllGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC All [Beer]", Columns, null);

			Assert.IsTrue(p.Sql.Contains("BeerTable"));
			Assert.IsTrue(p.Sql.Contains("BeerIdTable"));
	
			Assert.IsTrue(p.Sql.Contains("SelectBeer"));
			Assert.IsTrue(p.Sql.Contains("InsertBeer"));
			Assert.IsTrue(p.Sql.Contains("UpdateBeer"));
			Assert.IsTrue(p.Sql.Contains("UpsertBeer"));
			Assert.IsTrue(p.Sql.Contains("DeleteBeer"));

			Assert.IsTrue(p.Sql.Contains("SelectBeers"));
			Assert.IsTrue(p.Sql.Contains("InsertBeers"));
			Assert.IsTrue(p.Sql.Contains("UpdateBeers"));
			Assert.IsTrue(p.Sql.Contains("UpsertBeers"));
			Assert.IsTrue(p.Sql.Contains("DeleteBeers"));

			Assert.IsTrue(p.Sql.Contains("Find"));
		}

		[Test]
		public void TestSelectGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC Select [Beer] SelectBeer", Columns, null);

			Assert.AreEqual("CREATE PROCEDURE [SelectBeer]\r\n(\r\n\t@ID int\r\n)\r\nAS\r\nSELECT * FROM [Beer] WHERE \r\n\t[ID]=@ID\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void TestInsertGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", Columns, null);

			Assert.AreEqual("CREATE PROCEDURE [InsertBeer]\r\n(\r\n\t@Name varchar(256),\r\n\t@OriginalGravity decimal(18,2)\r\n)\r\nAS\r\nINSERT INTO [Beer]\r\n(\r\n\t[Name],\r\n\t[OriginalGravity]\r\n)\r\nOUTPUT\r\n\tInserted.[ID]\r\nVALUES\r\n(\r\n\t@Name,\r\n\t@OriginalGravity\r\n)\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void TestUpdateGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC Update [Beer] UpdateBeer", Columns, null);

			Assert.AreEqual("CREATE PROCEDURE [UpdateBeer]\r\n(\r\n\t@ID int,\r\n\t@Name varchar(256),\r\n\t@OriginalGravity decimal(18,2)\r\n)\r\nAS\r\nUPDATE [Beer] SET\r\n\t[Name]=@Name,\r\n\t[OriginalGravity]=@OriginalGravity\r\nWHERE\r\n\t[ID]=@ID\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void TestDeleteGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC Delete [Beer] DeleteBeer", Columns, null);

			Assert.AreEqual("CREATE PROCEDURE [DeleteBeer]\r\n(\r\n\t@ID int\r\n)\r\nAS\r\nDELETE FROM [Beer] WHERE\r\n\t[ID]=@ID\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void InsertWithNoIdentitiesShouldOmitOutputStatement()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = false },
				new ColumnDefinition() { Name = "OriginalGravity", SqlType = "decimal(18,2)", IsKey = false },
			});

			AutoProc p = new AutoProc("AUTOPROC Insert [Beer] InsertBeer", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [InsertBeer]\r\n(\r\n\t@ID int,\r\n\t@Name varchar(256),\r\n\t@OriginalGravity decimal(18,2)\r\n)\r\nAS\r\nINSERT INTO [Beer]\r\n(\r\n\t[ID],\r\n\t[Name],\r\n\t[OriginalGravity]\r\n)\r\nVALUES\r\n(\r\n\t@ID,\r\n\t@Name,\r\n\t@OriginalGravity\r\n)\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void UpdateWithOnlyKeysShouldRaiseError()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
				new ColumnDefinition() { Name = "Name", SqlType = "varchar(256)", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Update [Beer] UpdateBeer", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [UpdateBeer]\r\n(\r\n\t@ID int,\r\n\t@Name varchar(256)\r\n)\r\nAS\r\nRAISERROR (N'There are no UPDATEable fields on [Beer]', 18, 0)\r\n\r\nGO\r\n", p.Sql);
		}
		#endregion

		#region Multiple CRUD Generation Tests
		[Test]
		public void TestTableTypeGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC Table [Beer]", Columns, null);

			Assert.AreEqual("CREATE TYPE [BeerTable]\r\nAS TABLE\r\n(\r\n\t[ID] int,\r\n\t[Name] varchar(256),\r\n\t[OriginalGravity] decimal(18,2)\r\n)\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void TestInsertManyGeneration()
		{
			AutoProc p = new AutoProc("AUTOPROC InsertMany [Beer]", Columns, null);

			Assert.AreEqual("CREATE PROCEDURE [InsertBeers] (@Beer [BeerTable] READONLY)\r\nAS\r\nINSERT INTO [Beer]\r\n(\r\n\t[Name],\r\n\t[OriginalGravity]\r\n)\r\nOUTPUT\r\n\tInserted.[ID]\r\nSELECT\r\n\t[Name],\r\n\t[OriginalGravity]\r\nFROM @Beer\r\n\r\nGO\r\n", p.Sql);
		}
		#endregion

		#region Find Tests
		[Test]
		public void ExecuteAsOwnerTrue()
		{
			AutoProc p = new AutoProc("AUTOPROC Find [Beer] ExecuteAsOwner=true", Columns, null);

			Assert.IsTrue(p.Sql.Contains("WITH EXECUTE AS OWNER"));
		}

		[Test]
		public void ExecuteAsOwnerFalse()
		{
			AutoProc p = new AutoProc("AUTOPROC Find [Beer] ExecuteAsOwner=false", Columns, null);

			Assert.IsFalse(p.Sql.Contains("WITH EXECUTE AS OWNER"));
		}

		[Test]
		public void ExecuteAsOwnerNull()
		{
			AutoProc p = new AutoProc("AUTOPROC Find [Beer]", Columns, null);

			Assert.IsFalse(p.Sql.Contains("WITH EXECUTE AS OWNER"));
		}
		#endregion

		#region Singular Tests
		[Test]
		public void TestSingularSqlGeneration()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Delete [People]", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [DeletePerson]\r\n(\r\n\t@ID int\r\n)\r\nAS\r\nDELETE FROM [People] WHERE\r\n\t[ID]=@ID\r\n\r\nGO\r\n", p.Sql);
		}

		[Test]
		public void TestSingularOverride()
		{
			Mock<IColumnDefinitionProvider> columns = new Mock<IColumnDefinitionProvider>();
			columns.Setup(c => c.GetColumns(It.IsAny<string>())).Returns(new List<ColumnDefinition>()
			{
				new ColumnDefinition() { Name = "ID", SqlType = "int", IsKey = true },
			});

			AutoProc p = new AutoProc("AUTOPROC Delete [People] Single=Foo", columns.Object, null);

			Assert.AreEqual("CREATE PROCEDURE [DeleteFoo]\r\n(\r\n\t@ID int\r\n)\r\nAS\r\nDELETE FROM [People] WHERE\r\n\t[ID]=@ID\r\n\r\nGO\r\n", p.Sql);
		}
		#endregion
	}
}
