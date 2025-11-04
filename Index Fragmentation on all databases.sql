create table #temp_if(
 [DBName] nvarchar(128) NULL,
 [SchemaName] nvarchar(128) NULL,
 [ObjectName] nvarchar(128) NULL,
 [IndexName] nvarchar(128) NULL,
 [fragmentation] numeric(38,35) NULL,
 [page_count] bigint NULL);

 exec master.sys.sp_msforeachdb 'USE [?];
insert into  #temp_if
select ''?'' as [DbName],
dbschemas.[name], 
dbtables.[name], 
dbindexes.[name],
indexstats.avg_fragmentation_in_percent,
indexstats.page_count
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS indexstats
INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]
INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]
INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
AND indexstats.index_id = dbindexes.index_id
WHERE indexstats.database_id = DB_ID() and indexstats.page_count > 1000
and indexstats.avg_fragmentation_in_percent > 5
and dbindexes.[name] IS NOT NULL;'

select * from  #temp_if
order by page_count desc;

drop table  #temp_if;