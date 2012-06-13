CREATE TABLE [Beer] (
	[ID] [int] IDENTITY,
	[Name] [nvarchar](128) NOT NULL,
	[Flavor] [nvarchar](128) NULL,
	[OriginalGravity] [decimal](18, 2) NULL,
	[Details][varchar](MAX)
) ON [PRIMARY]
GO
ALTER TABLE [Beer] WITH NOCHECK ADD CONSTRAINT [PK_Beer] PRIMARY KEY NONCLUSTERED
(
	[ID], [Name]	
)
GO
-- AUTOPROC All [Beer]
GO

CREATE TABLE [dbo].[Glasses](
	[Name] [varchar](32) NULL,
	[Ounces] [int] NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Servings](
	[When] [datetime] NULL,
	[BeerName] [varchar](32) NULL,
	[GlassName] [varchar](32) NULL
) ON [PRIMARY]
GO

CREATE TYPE [dbo].[BeerTable] AS TABLE(
	[Name] [nvarchar](128) NULL,
	[Flavor] [nvarchar](128) NULL,
	[OriginalGravity] [decimal](18, 2) NULL
)
GO

CREATE TYPE [dbo].[BeerNameTable] AS TABLE(
	[Name] [nvarchar](128) NULL
)
GO

CREATE PROCEDURE [dbo].[GetServings]
AS
	SELECT [When], b.*, g.*
		FROM Servings s
		JOIN Beer b ON (s.BeerName = b.Name)
		JOIN Glasses g ON (s.GlassName = g.Name)
GO

CREATE PROCEDURE [dbo].[GetBeer] (@Names [BeerNameTable] READONLY)
AS
	SELECT * FROM Beer WHERE Name IN (SELECT Name FROM @Names)
GO

CREATE PROCEDURE [dbo].[FindBeer]
	@Name [nvarchar](128)
AS
	SELECT * FROM Beer WHERE Name LIKE @Name
GO