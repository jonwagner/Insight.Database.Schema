CREATE TABLE [Servings]
(
	[ID] [int] IDENTITY,
	[BeerID] [int] NOT NULL,
	[GlassesID] [int] NOT NULL,
	[When] [datetime] NULL,
) ON [PRIMARY]
GO

ALTER TABLE [Servings] WITH NOCHECK ADD CONSTRAINT [PK_Servings] PRIMARY KEY NONCLUSTERED
(
	[ID]
)
GO

-- AUTOPROC All [Servings]
GO