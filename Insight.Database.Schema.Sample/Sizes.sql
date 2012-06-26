CREATE TABLE [Sizes]
(
	[BeerID] [int] NOT NULL,
	[GlassID] [int] NOT NULL,
) ON [PRIMARY]
GO

ALTER TABLE [Sizes] WITH NOCHECK ADD CONSTRAINT [PK_Sizes] PRIMARY KEY NONCLUSTERED
(
	[BeerID],
	[GlassID]
)
GO

-- AUTOPROC All [Sizes]
GO