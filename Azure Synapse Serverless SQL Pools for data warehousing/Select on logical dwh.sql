-- For reference only.
-- The script won't work as you don't have the parquet files.

--Select a file
SELECT TOP 1000 *
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_Customer/20220413_Customer.parquet',
        FORMAT='PARQUET'
    ) AS [result]

--Select multiple files
SELECT TOP 1000 *
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_Customer/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]

--Distinct
SELECT DISTINCT DateModified
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_Customer/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]

--Group by
SELECT  Id
      , MAX(DateModified) AS DateModified
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_Customer/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]
GROUP BY Id


--Aggregation
SELECT *
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_SalesorderDetail/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]

SELECT SUM(Linetotal)
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_SalesorderDetail/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]

SELECT SalesorderHeader_Id
     , SUM(Linetotal)
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_SalesorderDetail/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]
GROUP BY SalesorderHeader_Id

--Join
SELECT soh.Ordernumber
     , SUM(Linetotal)
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_SalesorderHeader/*.parquet',
        FORMAT='PARQUET'
    ) AS soh
    JOIN 
        OPENROWSET(
            BULK 'https://[your storage accountname].dfs.core.windows.net/raw/Fanstore_dbo_SalesorderDetail/*.parquet',
            FORMAT='PARQUET'
        ) AS sod ON soh.Id = sod.SalesorderHeader_Id
GROUP BY soh.Ordernumber
ORDER BY soh.Ordernumber