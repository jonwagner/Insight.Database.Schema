CREATE TABLE [Receipts]
(
	[ID] [int] NOT NULL,
	[Name] [nvarchar](128) NOT NULL
) ON [PRIMARY]
GO

ALTER TABLE [Receipts] WITH NOCHECK ADD CONSTRAINT [PK_Receipts] PRIMARY KEY NONCLUSTERED
(
	[ID]
)
GO

-- AUTOPROC All [Receipts]
GO