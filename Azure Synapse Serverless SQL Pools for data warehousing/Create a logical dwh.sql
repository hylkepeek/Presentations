-- Create a logical datawarehouse
-- The script works with the following folder structure in your storage account:
--	Contrainer 'raw'
--			Folders	'Fanstore_dbo_Customer'
--				    'Fanstore_dbo_Product'
--					'Fanstore_dbo_ProductGroup'
--					'Fanstore_dbo_SalesorderDetail'
--					'Fanstore_dbo_SalesorderHeader'
--	Contrainer 'transformed'
--		Folder 'Datamart'
--			Folder 'Dim_Customer'
--				   'Dim_Product'
--				   'Dim_ProductGroup'
--				   'Dim_SalesorderHeader'
--				   'Fact_SalesorderDetail'



--Database
	CREATE DATABASE [your database name]
		COLLATE Latin1_General_100_BIN2_UTF8
	GO
	USE [your database name]
	GO

--Data source (data lake, cosmos)
	CREATE EXTERNAL DATA SOURCE rawdata
	WITH ( LOCATION = 'https://[your storage accountname].dfs.core.windows.net/raw')
	GO
	CREATE EXTERNAL DATA SOURCE transformed
	WITH ( LOCATION = 'https://[your storage accountname].dfs.core.windows.net/transformed')
	GO

--Data types (parquet, csv, ...)
	CREATE EXTERNAL FILE FORMAT ParquetFormat WITH (  FORMAT_TYPE = PARQUET );
	GO

--CREATE RAW-LAYER - External tables to access ADLS-raw files
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[fan_SalesorderHeader]') AND type in (N'U'))
	DROP EXTERNAL TABLE [raw].[fan_SalesorderHeader]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[fan_SalesorderDetail]') AND type in (N'U'))
	DROP EXTERNAL TABLE [raw].[fan_SalesorderDetail]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[fan_Productgroup]') AND type in (N'U'))
	DROP EXTERNAL TABLE [raw].[fan_Productgroup]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[fan_Product]') AND type in (N'U'))
	DROP EXTERNAL TABLE [raw].[fan_Product]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[fan_Customer]') AND type in (N'U'))
	DROP EXTERNAL TABLE [raw].[fan_Customer]
	GO
	IF  EXISTS (SELECT * FROM sys.schemas s WHERE s.name = 'raw')
	DROP SCHEMA [raw];
	GO
	CREATE SCHEMA [raw];
	GO

	CREATE EXTERNAL TABLE [raw].[fan_SalesorderHeader]
	(
		[Id] [int],
		[Ordernumber] [varchar](20),
		[Orderdatetime] [datetime2](0),
		[Statuscode] [varchar](10),
		[Customer_Id] [int],
		[DateCreated] [datetime2](0),
		[DateModified] [datetime2](0)
	)
	WITH (DATA_SOURCE = [rawdata],LOCATION = N'Fanstore_dbo_SalesorderHeader/*.parquet',FILE_FORMAT = [ParquetFormat])
	GO
	CREATE EXTERNAL TABLE [raw].[fan_SalesorderDetail]
	(
		[Id] [int],
		[Orderdetailnumber] [varchar](20),
		[Quantity] [decimal](6, 2),
		[Statuscode] [varchar](10),
		[Linetotal] [decimal](6, 2),
		[Unitprice] [decimal](6, 2),
		[Unit_Discount] [decimal](6, 2),
		[Vat_Total] [decimal](6, 2),
		[Product_Id] [int],
		[SalesorderHeader_Id] [int],
		[DateCreated] [datetime2](0),
		[DateModified] [datetime2](0)
	)
	WITH (DATA_SOURCE = [rawdata],LOCATION = N'Fanstore_dbo_SalesorderDetail/*.parquet',FILE_FORMAT = [ParquetFormat])
	GO
	CREATE EXTERNAL TABLE [raw].[fan_Productgroup]
	(
		[Id] [int],
		[Name] [varchar](30),
		[Description] [varchar](50),
		[Statuscode] [varchar](10),
		[DateCreated] [datetime2](0),
		[DateModified] [datetime2](0)
	)
	WITH (DATA_SOURCE = [rawdata],LOCATION = N'Fanstore_dbo_Productgroup/*.parquet',FILE_FORMAT = [ParquetFormat])
	GO
	CREATE EXTERNAL TABLE [raw].[fan_Product]
	(
		[Id] [int],
		[Name] [nvarchar](30),
		[Purchaseprice] [decimal](6, 2),
		[Salesprice] [decimal](6, 2),
		[Statuscode] [varchar](10),
		[Productgroup_Id] [int],
		[DateCreated] [datetime2](0),
		[DateModified] [datetime2](0)
	)
	WITH (DATA_SOURCE = [rawdata],LOCATION = N'Fanstore_dbo_Product/*.parquet',FILE_FORMAT = [ParquetFormat])
	GO
	CREATE EXTERNAL TABLE [raw].[fan_Customer]
	(
		[Id] [int],
		[Name] [varchar](30),
		[Address_Composite] [varchar](100),
		[City] [varchar](100),
		[Country] [varchar](100),
		[State_or_Province] [varchar](20),
		[Postalcode] [varchar](50),
		[Statuscode] [varchar](50),
		[DateCreated] [datetime2](0),
		[DateModified] [datetime2](0)
	)
	WITH (DATA_SOURCE = [rawdata],LOCATION = N'Fanstore_dbo_Customer/*.parquet',FILE_FORMAT = [ParquetFormat])
	GO

--CREATE COMBINE-LAYER - Views to access raw-layer
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[combine].[SalesorderHeader]') AND type in (N'V'))
	DROP VIEW [combine].[SalesorderHeader]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[combine].[SalesorderDetail]') AND type in (N'V'))
	DROP VIEW [combine].[SalesorderDetail]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[combine].[ProductGroup]') AND type in (N'V'))
	DROP VIEW [combine].[ProductGroup]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[combine].[Product]') AND type in (N'V'))
	DROP VIEW [combine].[Product]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[combine].[Customer]') AND type in (N'V'))
	DROP VIEW [combine].[Customer]
	GO
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = 'combine')
	DROP SCHEMA combine;
	GO
	CREATE SCHEMA combine;
	GO

	CREATE VIEW [combine].[SalesorderHeader]
	AS
	SELECT [Id]
		  ,[Ordernumber]
		  ,[Orderdatetime]
		  ,[Statuscode]
		  ,[Customer_Id]
		  ,[DateCreated]
		  ,[DateModified]
	  FROM [raw].[fan_SalesorderHeader]
	GO
	CREATE VIEW [combine].[SalesorderDetail]
	AS
	SELECT [Id]
		  ,[Orderdetailnumber]
		  ,[Quantity]
		  ,[Statuscode]
		  ,[Linetotal]
		  ,[Unitprice]
		  ,[Unit_Discount]
		  ,[Vat_Total]
		  ,[Product_Id]
		  ,[SalesorderHeader_Id]
		  ,[DateCreated]
		  ,[DateModified]
	  FROM [raw].[fan_SalesorderDetail]
	GO
	CREATE VIEW [combine].[ProductGroup]
	AS
	SELECT [Id]
		  ,[Name]
		  ,[Description]
		  ,[Statuscode]
		  ,[DateCreated]
		  ,[DateModified]
	FROM [raw].[fan_Productgroup]
	GO
	CREATE VIEW [combine].[Product]
	AS
	SELECT [Id]
		  ,[Name]
		  ,[Purchaseprice]
		  ,[Salesprice]
		  ,[Statuscode]
		  ,[Productgroup_Id]
		  ,[DateCreated]
		  ,[DateModified]
	FROM [raw].[fan_Product]
	GO
	CREATE VIEW [combine].[Customer]
	AS
	SELECT [Id]
		  ,[Name]
		  ,[Address_Composite]
		  ,[City]
		  ,[Country]
		  ,[State_or_Province]
		  ,[Postalcode]
		  ,[Statuscode]
		  ,[DateCreated]
		  ,[DateModified]
	FROM raw.fan_Customer
	GO

--CREATE DATAMART-LAYER - Stored procs to CETAS external tables
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[SalesorderHeader]') AND type in (N'U'))
	DROP EXTERNAL TABLE [dm].[SalesorderHeader]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[SalesorderDetail]') AND type in (N'U'))
	DROP EXTERNAL TABLE [dm].[SalesorderDetail]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[ProductGroup]') AND type in (N'U'))
	DROP EXTERNAL TABLE [dm].[ProductGroup]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[Product]') AND type in (N'U'))
	DROP EXTERNAL TABLE [dm].[Product]
	GO
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[Customer]') AND type in (N'U'))
	DROP EXTERNAL TABLE [dm].[Customer]
	GO
	IF  EXISTS (SELECT * FROM sys.schemas s WHERE s.name = 'dm')
	DROP SCHEMA [dm];
	GO
	CREATE SCHEMA [dm];
	GO

	CREATE OR ALTER PROCEDURE [dbo].[drop_external_table_if_exists] @tablename varchar(100), @schemaname varchar(20)
	AS BEGIN
		IF (0 <> (SELECT COUNT(*) FROM sys.external_tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = @tablename AND s.name = @schemaname ))
		BEGIN
			DECLARE @drop_stmt NVARCHAR(200) = N'DROP EXTERNAL TABLE ' + @schemaname + '.' + @tablename; 
			EXEC sp_executesql @tsql = @drop_stmt;
		END
	END
	GO
	CREATE OR ALTER PROCEDURE [dm].[Load_Customer]
	AS
		EXEC dbo.drop_external_table_if_exists 'D_Customer', 'dm'

		DECLARE @location varchar(100) = ''
		SET @location = CONCAT('Datamart/Dim_Customer/', FORMAT (SYSDATETIME(), 'yyyyMMdd-HHmm') )

		DECLARE @sqlCetas nvarchar(4000)

		SET @sqlCetas = 
		'
		  CREATE EXTERNAL TABLE dm.D_Customer
		  WITH (
				location = ''' + @location + ''',
				data_source = transformed,
				file_format = ParquetFormat
		  )  
		  AS
		  SELECT  CAST(ROW_NUMBER() OVER(ORDER BY c.Id, c.DateModified) AS INT) AS PK_Customer
				, c.Id
				, c.Name
				, c.Address_Composite
				, c.City
				, c.Country
				, c.State_or_Province
				, c.Postalcode
				, c.Statuscode
				, c.DateModified AS ValidFrom
				, COALESCE(LEAD(c.DateModified) OVER (PARTITION BY c.Id ORDER BY c.DateModified), ''2099-12-31'') AS ValidTo
		  FROM combine.Customer c
		'
		EXEC sp_executesql @sqlCetas

	GO
	CREATE OR ALTER PROCEDURE [dm].[Load_Product]
	AS
  		EXEC drop_external_table_if_exists 'D_Product', 'dm'

		DECLARE @location varchar(100) = ''
		SET @location = CONCAT('Datamart/Dim_Product/', FORMAT (SYSDATETIME(), 'yyyyMMdd-HHmm') )

		DECLARE @sqlCetas nvarchar(4000)

		SET @sqlCetas = 
		'
		  CREATE EXTERNAL TABLE dm.D_Product
		  WITH (
				location = ''' + @location + ''',
				data_source = transformed,
				file_format = ParquetFormat
		  )  
		  AS
		  SELECT CAST(ROW_NUMBER() OVER(ORDER BY p.Id, p.DateModified) AS INT) AS PK_Product
				, p.Id
				, p.Name
				, p.Purchaseprice
				, p.Salesprice
				, p.Statuscode
				, p.Productgroup_Id
				, p.DateCreated
				, p.DateModified AS ValidFrom
				, COALESCE(LEAD(p.DateModified) OVER (PARTITION BY p.Id ORDER BY p.DateModified), ''2099-12-31'') AS ValidTo
		  FROM combine.Product p
		'
		EXEC sp_executesql @sqlCetas
	GO
	CREATE OR ALTER PROCEDURE [dm].[Load_ProductGroup]
	AS
		EXEC drop_external_table_if_exists 'D_ProductGroup', 'dm'

		DECLARE @location varchar(100) = ''
		SET @location = CONCAT('Datamart/Dim_ProductGroup/', FORMAT (SYSDATETIME(), 'yyyyMMdd-HHmm') )

		DECLARE @sqlCetas nvarchar(4000)

		SET @sqlCetas = 
		'
		  CREATE EXTERNAL TABLE dm.D_ProductGroup
		  WITH (
				location = ''' + @location + ''',
				data_source = transformed,
				file_format = ParquetFormat
		  )  
		  AS
		  SELECT  CAST(ROW_NUMBER() OVER(ORDER BY pg.Id, pg.DateModified) AS INT) AS PK_ProductGroup
				, pg.Id
				, pg.Name
				, pg.Description
				, pg.Statuscode
				, pg.DateModified AS ValidFrom
				, COALESCE(LEAD(pg.DateModified) OVER (PARTITION BY pg.Id ORDER BY pg.DateModified), ''2099-12-31'') AS ValidTo
		  FROM combine.ProductGroup pg
		'
		EXEC sp_executesql @sqlCetas
	GO
	CREATE OR ALTER PROCEDURE [dm].[Load_SalesorderHeader]
	AS
  		EXEC drop_external_table_if_exists 'D_SalesorderHeader', 'dm'

		DECLARE @location varchar(100) = ''
		SET @location = CONCAT('Datamart/Dim_SalesorderHeader/', FORMAT (SYSDATETIME(), 'yyyyMMdd-HHmm') )

		DECLARE @sqlCetas nvarchar(4000)

		SET @sqlCetas = 
		'
		  CREATE EXTERNAL TABLE dm.D_SalesorderHeader
		  WITH (
				location = ''' + @location + ''',
				data_source = transformed,
				file_format = ParquetFormat
		  )  
		  AS
		  SELECT  CAST(ROW_NUMBER() OVER(ORDER BY soh.Id, soh.DateModified) AS INT) AS PK_SalesOrderHeader
				, soh.Id
				, soh.Ordernumber
				, soh.Orderdatetime
				, soh.Statuscode
				, soh.Customer_Id
				, soh.DateModified AS ValidFrom
				, COALESCE(LEAD(soh.DateModified) OVER (PARTITION BY soh.Id ORDER BY soh.DateModified), ''2099-12-31'') AS ValidTo
		  FROM combine.SalesorderHeader soh
		'
		EXEC sp_executesql @sqlCetas
	GO

	CREATE OR ALTER PROCEDURE [dm].[Load_SalesorderDetail]
	AS
		EXEC drop_external_table_if_exists 'F_SalesorderDetail', 'dm'

		DECLARE @location varchar(100) = ''
		SET @location = CONCAT('Datamart/Fact_SalesorderDetail/', FORMAT (SYSDATETIME(), 'yyyyMMdd-HHmm') )

		DECLARE @sqlCetas nvarchar(4000)

		SET @sqlCetas = 
		'
		  CREATE EXTERNAL TABLE dm.F_SalesorderDetail
		  WITH (
				location = ''' + @location + ''',
				data_source = transformed,
				file_format = ParquetFormat
		  )  
		  AS
		  SELECT  soh.PK_SalesOrderHeader AS FK_SalesOrderHeader
				, p.PK_Product AS FK_Product
				, pg.PK_ProductGroup AS FK_ProductGroup
				, c.PK_Customer AS FK_Customer
				, sod.Orderdetailnumber
				, sod.Quantity
				, sod.Statuscode
				, sod.Linetotal
				, sod.Unitprice
				, sod.Unit_Discount
				, sod.Vat_Total
		  FROM combine.SalesorderDetail sod
			JOIN dm.D_SalesorderHeader soh ON sod.SalesorderHeader_Id = soh.Id
			LEFT JOIN dm.D_Product p ON sod.Product_Id = p.Id AND soh.Orderdatetime BETWEEN p.ValidFrom AND p.ValidTo
			LEFT JOIN dm.D_ProductGroup pg ON p.Productgroup_Id = pg.Id AND soh.Orderdatetime BETWEEN pg.ValidFrom AND pg.ValidTo
			LEFT JOIN dm.D_Customer c ON soh.Customer_Id = c.Id AND soh.Orderdatetime BETWEEN c.ValidFrom AND c.ValidTo
		'
		EXEC sp_executesql @sqlCetas
	GO

