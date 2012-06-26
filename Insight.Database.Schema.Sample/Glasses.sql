CREATE TABLE [Glasses]
(
	[ID] [int] IDENTITY,
	[Name] [varchar](32) NULL,
	[Ounces] [int] NULL
) ON [PRIMARY]
GO

ALTER TABLE [Glasses] WITH NOCHECK ADD CONSTRAINT [PK_Glasses] PRIMARY KEY NONCLUSTERED
(
	[ID]
)
GO

-- AUTOPROC All [Glasses]
GO
