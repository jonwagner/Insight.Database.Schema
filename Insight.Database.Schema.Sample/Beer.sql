CREATE TABLE [Beer]
(
	[ID] [int] IDENTITY,
	[Name] [nvarchar](128) NOT NULL,
	[Flavor] [nvarchar](128) NULL,
	[OriginalGravity] [decimal](18, 2) NULL,
	[Details] [varchar](MAX),
	[Hoppiness] [int] DEFAULT(6),
	[Yumminess] AS [Hoppiness] * 2,
	[RowVersion] [timestamp]
) ON [PRIMARY]
GO

ALTER TABLE [Beer] WITH NOCHECK ADD CONSTRAINT [PK_Beer] PRIMARY KEY NONCLUSTERED
(
	[ID]	
)
GO

-- AUTOPROC All,Optimistic [Beer] 
GO

GRANT ALL ON SelectBeer TO [public]
GO