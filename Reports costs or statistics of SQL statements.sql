-- *************************************************************************************************
-- Author j.roberts 
-- Date 11 July 2024
-- Reports costs/statistics of SQL statements for given time on given database
-- *************************************************************************************************
GO  
DECLARE @Database      sysname		= NULL, -- Set the database name or set as null for all databases
        @MonitorPeriod varchar(10)	= '0:05:00' -- 1 hour -- set this to the time period you want to monitor for

BEGIN    
    
    SET NOCOUNT ON;
    SET ARITHABORT OFF;
    SET ANSI_WARNINGS OFF;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;    

       CREATE TABLE #CombinedResults
    (
        DBName sysname,
        QueryText varchar(MAX),
        Execution_count bigint,
        total_cpu_time bigint,
        total_IO bigint,
        total_physical_reads bigint,
        total_logical_reads bigint,
        total_logical_writes bigint,
        total_elapsed_time bigint,
        Identifier tinyint
       ,plan_handle varbinary(64) not null
       ,last_execution_time datetime
    );

    DECLARE @Iteration tinyint = 1;    
    WHILE @Iteration <= 2 BEGIN
        ;WITH PlanStats AS
        (
        SELECT st.dbid,
               st.text AS QueryText,
               cp.plan_handle,
               MAX(cp.usecounts) AS Execution_count,
               SUM(qs.total_worker_time) AS total_cpu_time,
               SUM(qs.total_physical_reads + qs.total_logical_reads + qs.total_logical_writes) AS total_IO,
               SUM(qs.total_physical_reads) AS total_physical_reads,
               SUM(qs.total_logical_reads) AS total_logical_reads,
               SUM(qs.total_logical_writes) AS total_logical_writes,
               SUM(qs.total_elapsed_time) AS total_elapsed_time,
               MAX(qs.last_execution_time) AS last_execution_time
        FROM sys.dm_exec_cached_plans cp
        INNER JOIN sys.dm_exec_query_stats qs 
                ON cp.plan_handle = qs.plan_handle
        CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) st
        WHERE DB_NAME(st.dbid) IS NOT NULL
         -- AND DB_NAME(st.dbid) =	coalesce(	@Database,
		--										DB_NAME(st.dbid))	
		  AND DB_NAME(st.dbid)	NOT IN	(	'tempdb',
											'master',
											'msdb')
        GROUP BY st.dbid, st.text, cp.plan_handle
        )
        INSERT INTO #CombinedResults
        SELECT DB_NAME(ps.dbid) AS DBName,
               ps.QueryText,
               SUM(ps.Execution_count) AS Execution_count,
               SUM(ps.total_cpu_time) AS total_cpu_time,
               SUM(ps.total_IO) AS total_IO,
               SUM(ps.total_physical_reads) AS total_physical_reads,
               SUM(ps.total_logical_reads) AS total_logical_reads,
               SUM(ps.total_logical_writes) AS total_logical_writes,
               SUM(ps.total_elapsed_time) AS total_elapsed_time,
               @Iteration AS Identifier
              ,ps.plan_handle
              ,max(ps.last_execution_time)
        FROM PlanStats ps
        GROUP BY ps.dbid, ps.QueryText, plan_handle;

        -- Wait for the specified monitoring period during the first iteration
        IF @Iteration = 1 BEGIN
            WAITFOR DELAY @MonitorPeriod;
        END

        SET @Iteration = @Iteration + 1;
    
    END        
  

-- ************************************************************************************************
-- Results
-- ************************************************************************************************

    SELECT ISNULL(A.DBName, B.DBName)                                            AS DBName,    
           ISNULL(A.QueryText, B.QueryText)                                      AS QueryText,    
           ISNULL(A.Execution_count, 0) - ISNULL(B.Execution_count, 0)           AS Execution_count,    
           ISNULL(A.total_cpu_time, 0) - ISNULL(B.total_cpu_time, 0)             AS total_cpu_time,            
           (ISNULL(A.total_cpu_time,0) - ISNULL(B.total_cpu_time, 0)) / NULLIF(ISNULL(A.Execution_count,0) - ISNULL(B.Execution_count, 0), 0) AS avg_cpu_time,
           ISNULL(A.total_IO, 0) - ISNULL(B.total_IO, 0)                         AS total_IO,             
           (ISNULL(A.total_IO,0) - ISNULL(B.total_IO, 0)) / NULLIF(ISNULL(A.execution_count,0) - ISNULL(B.execution_count, 0), 0) AS avg_total_IO,
           ISNULL(A.total_physical_reads, 0) - ISNULL(B.total_physical_reads, 0) AS total_physical_reads,         
           (ISNULL(A.total_physical_reads,0) - ISNULL(B.total_physical_reads, 0)) / NULLIF(ISNULL(A.execution_count,0) - ISNULL(B.execution_count, 0), 0) AS avg_physical_read,
           ISNULL(A.total_logical_reads, 0) - ISNULL(B.total_logical_reads, 0)   AS total_logical_reads,             
           (ISNULL(A.total_logical_reads, 0) - ISNULL(B.total_logical_reads, 0)) / NULLIF(ISNULL(A.execution_count,0) - ISNULL(B.execution_count, 0), 0) AS avg_logical_read,
           ISNULL(A.total_logical_writes, 0) - ISNULL(B.total_logical_writes, 0) AS total_logical_writes,             
           (ISNULL(A.total_logical_writes, 0) - ISNULL(B.total_logical_writes, 0)) / NULLIF(ISNULL(A.execution_count,0) - ISNULL(B.execution_count, 0), 0) AS avg_logical_writes,
           ISNULL(A.total_elapsed_time, 0) - ISNULL(B.total_elapsed_time, 0)     AS total_elapsed_time,            
           (ISNULL(A.total_elapsed_time, 0) - ISNULL(B.total_elapsed_time, 0)) / NULLIF(ISNULL(A.execution_count, 0) - ISNULL(B.execution_count, 0), 0) AS avg_elapsed_time,
           ISNULL(A.last_execution_time, A.last_execution_time) last_execution_time
      FROM (SELECT * FROM #CombinedResults WHERE Identifier = 2) A
      LEFT JOIN (SELECT * FROM #CombinedResults WHERE Identifier = 1) B
             ON A.DBName = B.DBName
            AND A.QueryText = B.QueryText
            AND A.plan_handle = B.plan_handle
     WHERE ISNULL(A.Execution_count, 0) - ISNULL(B.Execution_count, 0) <>	0				and
		   ISNULL(A.QueryText, B.QueryText)								is	not	null
     ORDER BY 4 DESC;     

    DROP TABLE #CombinedResults;
END
GO