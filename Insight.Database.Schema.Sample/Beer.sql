CREATE TABLE [Beer]
(
	[ID] [int] IDENTITY,
	[Name] [nvarchar](128) NOT NULL,
	[Flavor] [nvarchar](128) NULL,
	[OriginalGravity] [decimal](18, 2) NULL,
	[Details][varchar](MAX)
) ON [PRIMARY]
GO

ALTER TABLE [Beer] WITH NOCHECK ADD CONSTRAINT [PK_Beer] PRIMARY KEY NONCLUSTERED
(
	[ID]	
)
GO

-- AUTOPROC All [Beer]
GO

GRANT ALL ON SelectBeer TO [public]
GO