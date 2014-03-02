# Insight.Database.Schema #

**Insight.Database.Schema** is a dead-simple easy to use SQL Server database script installer and "SQL code migrator".

- You write SQL schemas in SQL.
- You *don't* write schema change scripts.
- Insight will automatically calcualte the differences and apply them.
- Great for distributed development of SQL, since you don't have to worry about writing any upgrade code.

# Why You Want This #
- It just works. 
- Without a lot of effort or configuration. 
- It gets out of your way.
- SQL is SQL, not some mangled C# form of your data model.
- Works awesome with [Insight.Database](https://github.com/jonwagner/Insight.Database)!

## v2.0 Release ##
Huzzah! v2.0 is finally done!

Why is 2.0 better?

- Compatibility with SQL Azure, so you can deploy to the cloud.
- No dependency on SQL-SMO - SMO didn't play nicely with SQL Azure.
- Better dependency detection - when you modify something, it makes the minimum set of changes to the database.
- Column-level table updates - now columns can be added, removed, or modified without copying the entire data table.

Also...

- The code is a lot cleaner.
- More test cases.

# Documentation #

**See the [wiki](https://github.com/jonwagner/Insight.Database.Schema/wiki)!**

# Some Examples #

You can read these to get a flavor for the beer/code. But you should go over to the **[wiki](https://github.com/jonwagner/Insight.Database.Schema/wiki).**

## Getting Started ##
1. Get the nuGet package: [http://www.nuget.org/packages/Insight.Database.Schema](http://www.nuget.org/packages/Insight.Database.Schema)
1. Put your SQL code in SQL files.
1. Add the SQL files to your project as embedded resources.
1. Run the installer code (only a few lines below!).
1. Modify your SQL as needed.
1. Run the installer to automatically update your database.

## Make your SQL ##

So assume you have Beer.sql:

	CREATE TABLE Beer 
	(
		[ID] [int] IDENTITY, 
		[Name] [varchar](128)
	)
	GO
	CREATE PROC InsertBeer (@Name [varchar](128)) AS 
		INSERT INTO Beer (Name) 
			OUTPUT Inserted.ID VALUES (@Name)
	GO

## Load the Schema in your Setup code and Install it ##

Only a little code is needed to deploy your SQL.

	// load your SQL into a SchemaObjectCOllection
	SchemaObjectCollection schema = new SchemaObjectCollection();
	schema.Load("Beer.sql");

	// automatically create the database
	SchemaInstaller.CreateDatabase(connectionString);

	// automatically install it, or upgrade it
    using (SqlConnection connection = new SqlConnection (connectionString))
	{
		connection.Open();
		SchemaInstaller installer = new SchemaInstaller(connection);
		new SchemaEventConsoleLogger().Attach(installer);
		installer.Install("BeerGarten", schema);
	}

## Make some changes to your SQL ##

Go ahead. Just modify the SQL. Don't worry about writing upgrade scripts.

	CREATE TABLE Beer 
	(
		[ID] [int] IDENTITY NOT NULL, 
		[Name] [varchar](128) NOT NULL,
		[Description] [varchar](MAX)
	)
	GO
	CREATE PROC InsertBeer (@Name [varchar](128), @Description [varchar](MAX)) AS 
		INSERT INTO Beer (Name, Description) 
			OUTPUT Inserted.ID VALUES (@Name, @Description)
	GO

Now, run your setup program again. Insight will automatically calculate the differences between the existing database and your new database. Then it will only make the changes necessary to update your database.

## Get AutoProcs for Free ##

Automatically generate standard stored procedures for your tables and have them updated automatically if you change your schema.

Get all of these for FREE! Select, Insert, Update, Upsert, Delete, SelectMany, InsertMany, UpdateMany, UpsertMany, DeleteMany, Find.

	-- automatically generates Select/Insert/Update/Delete/Find and more
	-- AUTOPROC All [Beer]
	GO

## Use Insight.Database to Access Your Data ##

You don't have to use Insight.Database if you don't want to, but it's easy and fast.

To call your stored procedures (and SQL) easily. Use [Insight.Database](https://github.com/jonwagner/Insight.Database)!
It can even automatically generate a repository for all of the AutoProcs. You can select objects and send them back to the database with almost no effort!