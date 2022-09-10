--Prerequisites to execute these queries
--		You have a synapse workspace with a storage account linked to it.
--		The storage account has a blobcontainer with:
--			The file 'charts.csv' (download this file from https://www.kaggle.com/datasets/dhruvildave/spotify-charts).
--			The folder 'charts' with all the parquet files in it (see folder ChartsParquet in the gitrepo)
--		Replace [your storage accountname] with the name of the storage account
--		Replace [your container name] with the name of the blob container

--Parquet all columns
SELECT TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/[your container name]/charts/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
--Total size of data scanned is 11 megabytes, total size of data moved is 1 megabytes, total size of data written is 0 megabytes.


--csv all columns
SELECT TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/[your container name]/charts.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
--Total size of data scanned is 68 megabytes, total size of data moved is 1 megabytes, total size of data written is 0 megabytes.



--Parquet few columns
SELECT TOP 100 title, region, streams
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/[your container name]/charts/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
--Total size of data scanned is 4 megabytes, total size of data moved is 1 megabytes, total size of data written is 0 megabytes.


--csv few columns
SELECT TOP 100 title, region, streams
FROM
    OPENROWSET(
        BULK 'https://[your storage accountname].dfs.core.windows.net/[your container name]/charts.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
--Total size of data scanned is 68 megabytes, total size of data moved is 1 megabytes, total size of data written is 0 megabytes.
