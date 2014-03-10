CREATE SCHEMA foo
GO

CREATE ROLE MyRole
GO

CREATE LOGIN [TestLogin] WITH PASSWORD='test'
GO

CREATE USER [User] WITHOUT LOGIN
GO

-- PRESCRIPT [MyPreScript]
PRINT 'hello'
GO

-- SCRIPT [MyScript]
PRINT 'hello'
GO

CREATE MASTER KEY
   ENCRYPTION BY PASSWORD = 'pGFD4bb925DGvbd2439587y'
GO

CREATE CERTIFICATE [Certificate] 
   ENCRYPTION BY PASSWORD = 'pGFD4bb925DGvbd2439587y'
   WITH SUBJECT = 'Sammamish Shipping Records', 
   EXPIRY_DATE = '20221031';
GO

CREATE BROKER PRIORITY [TestPriority] FOR CONVERSATION
GO

CREATE MESSAGE TYPE [//Test/TestMessage]         
    VALIDATION = WELL_FORMED_XML
GO

CREATE CONTRACT [Contract] (
	[//Test/TestMessage] SENT BY INITIATOR
);
GO

CREATE QUEUE [Queue]
GO

CREATE SERVICE [Service] ON QUEUE [Queue]
GO

CREATE PARTITION FUNCTION PartitionFunction (int)
	AS RANGE LEFT
	FOR VALUES (1) 
GO

CREATE PARTITION SCHEME PartitionScheme
	AS PARTITION PartitionFunction
	ALL TO ([PRIMARY])
GO

-------------------------------------------------------------------------------------------------------------
-- things that can be in a schema
-------------------------------------------------------------------------------------------------------------
CREATE TYPE [dbo].[MyType] FROM [int]
GO

CREATE TABLE [dbo].[Table](
    [ID] [int] IDENTITY(1,1) NOT NULL,
    [Data] [varchar](100) NOT NULL,
	[Xml] [xml],
	[MyType] [MyType]
)
GO

-- AUTOPROC All [dbo].[Table]
GO

GRANT SELECT ON [dbo].[Table] TO [MyRole]
GO

CREATE TABLE [dbo].[Table2](
    [ID] [int] IDENTITY(1,1) NOT NULL,
	[ID1] [int]
)
GO

CREATE VIEW [dbo].[View]  AS
	SELECT * FROM [dbo].[Table]
GO

-- INDEXEDVIEW
CREATE VIEW [dbo].[IndexedView] WITH SCHEMABINDING AS
	SELECT ID, Data FROM [dbo].[Table]
GO

CREATE TRIGGER [dbo].[TRG_Table] ON [dbo].[Table]
	FOR INSERT 
AS
	PRINT 'Inserted'
GO

CREATE UNIQUE CLUSTERED INDEX [IX_IndexedView] ON [dbo].[IndexedView] (ID)
GO

ALTER TABLE [dbo].[Table] ADD CONSTRAINT [DF_Data] DEFAULT ((0)) FOR [Data]
GO

ALTER TABLE [dbo].[Table] ADD CONSTRAINT [PK_Table] PRIMARY KEY CLUSTERED ([ID])
GO

ALTER TABLE [dbo].[Table2] ADD CONSTRAINT [PK_Table2] PRIMARY KEY CLUSTERED ([ID])
GO

ALTER TABLE [dbo].[Table2] ADD CONSTRAINT [FK_Table] FOREIGN KEY ([ID1]) REFERENCES [dbo].[Table] (ID)
GO

ALTER TABLE [dbo].[Table] ADD CONSTRAINT [CT_Table] CHECK (Data > 'b')
GO

CREATE INDEX IX_Table ON [dbo].[Table] (Data)
GO

CREATE PRIMARY XML INDEX IX_Xml ON [dbo].[Table] ([Xml])
GO

CREATE XML INDEX IX_Xml2 ON [dbo].[Table] ([Xml])
	USING XML INDEX IX_Xml FOR PATH;
GO

CREATE PROC [dbo].[Proc] AS
	SELECT * FROM [dbo].[Table]
GO

GRANT EXEC ON [dbo].[Proc] TO [MyRole]
GO

CREATE FUNCTION [dbo].[Func] () RETURNS INT AS
BEGIN
	DECLARE @c [int]
	SELECT @c=COUNT(*) FROM [dbo].[Table]
	RETURN @c
END
GO

-------------------------------------------------------------------------------------------------------------
-- duplicate of above, but with a different schema
-------------------------------------------------------------------------------------------------------------
CREATE TYPE [foo].[MyType] FROM [int]
GO

CREATE TABLE [foo].[Table](
    [ID] [int] IDENTITY(1,1) NOT NULL,
    [Data] [varchar](100) NOT NULL,
	[Xml] [xml],
	[MyType] [MyType]
)
GO

-- AUTOPROC All [foo].[Table]
GO

GRANT SELECT ON [foo].[Table] TO [MyRole]
GO

CREATE TABLE [foo].[Table2](
    [ID] [int] IDENTITY(1,1) NOT NULL,
	[ID1] [int]
)
GO

CREATE VIEW [foo].[View]  AS
	SELECT * FROM [foo].[Table]
GO

-- INDEXEDVIEW
CREATE VIEW [foo].[IndexedView] WITH SCHEMABINDING AS
	SELECT ID, Data FROM [foo].[Table]
GO

CREATE TRIGGER [foo].[TRG_Table] ON [foo].[Table]
	FOR INSERT 
AS
	PRINT 'Inserted'
GO

CREATE UNIQUE CLUSTERED INDEX [IX_IndexedView] ON [foo].[IndexedView] (ID)
GO

ALTER TABLE [foo].[Table] ADD CONSTRAINT [DF_Data] DEFAULT ((0)) FOR [Data]
GO

ALTER TABLE [foo].[Table] ADD CONSTRAINT [PK_Table] PRIMARY KEY CLUSTERED ([ID])
GO

ALTER TABLE [foo].[Table2] ADD CONSTRAINT [PK_Table2] PRIMARY KEY CLUSTERED ([ID])
GO

ALTER TABLE [foo].[Table2] ADD CONSTRAINT [FK_Table] FOREIGN KEY ([ID1]) REFERENCES [foo].[Table] (ID)
GO

ALTER TABLE [foo].[Table] ADD CONSTRAINT [CT_Table] CHECK (Data > 'b')
GO

CREATE INDEX IX_Table ON [foo].[Table] (Data)
GO

CREATE PRIMARY XML INDEX IX_Xml ON [foo].[Table] ([Xml])
GO

CREATE XML INDEX IX_Xml2 ON [foo].[Table] ([Xml])
	USING XML INDEX IX_Xml FOR PATH;
GO

CREATE PROC [foo].[Proc] AS
	SELECT * FROM [foo].[Table]
GO

GRANT EXEC ON [foo].[Proc] TO [MyRole]
GO

CREATE FUNCTION [foo].[Func] () RETURNS INT AS
BEGIN
	DECLARE @c [int]
	SELECT @c=COUNT(*) FROM [foo].[Table]
	RETURN @c
END
GO
