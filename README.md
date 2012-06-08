# Insight.Database.Schema #

**Insight.Database.Schema** is a dead-simple easy to use SQL Server database script installer and "code migrator".

# Why You Want This #
- It just works. 
- Without a lot of effort or configuration. 
- It gets out of your way.
- SQL is SQL, not some mangled C# form of your data model
- Works awesome with [Insight.Database](https://github.com/jonwagner/Insight.Database)

# Documentation #

**See the [wiki](https://github.com/jonwagner/Insight.Database.Schema/wiki)!**

# Some Examples #

You can read these to get a flavor for the beer/code. But you should go over to the **[wiki](https://github.com/jonwagner/Insight.Database/wiki).**

## Getting Started ##
1. Get the nuGet package: [http://www.nuget.org/packages/Insight.Database.Schema](http://www.nuget.org/packages/Insight.Database.Schema)
1. Put your SQL code in SQL files.
1. Add the SQL files to your project as embedded resources.
1. Run the installer code (only a few lines below!).
1. Modify your SQL as needed.
1. Run the installer to automatically update your database.

## Make your SQL ##

So assume you have Beer.sql:

	CREATE PROC InsertBeer (@Name [varchar](128)) AS 
		INSERT INTO Beer (Name) OUTPUT Inserted.ID VALUES (@Name)
	GO
	CREATE TABLE Beer ([ID] [int] IDENTITY, [Name] [varchar](128))
	GO

## Add it as an Embedded Resource ##

1. Add Beer.sql to your project.
1. In Solution Explorer, right click on Beer.sql and choose Properties.
1. Change the Build Action to "Embedded Resource".

## Load the Schema in your Setup code and Install it ##

	// load all SQL files from the current assembly
	SchemaObjectCollection schema = new SchemaObjectCollection();
	schema.Load(System.Reflection.Assembly.GetExecutingAssembly());

	// automatically create the database and install it
	SchemaInstaller installer = new SchemaInstaller(connection.ConnectionString, connection.InitialCatalog);
	new SchemaEventConsoleLogger().Attach(installer);
	installer.CreateDatabase();
	installer.Install("BeerGarten", schema);

	// After you modify your SQL, just run this again!

## Use Insight.Database to Access Your Data ##

See [Insight.Database](https://github.com/jonwagner/Insight.Database)!