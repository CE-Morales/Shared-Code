alter	procedure	pa_obtiene_fragmentacion_indices	as

begin

declare	@comando	as	varchar(2000)

set	@comando				=		'USE [?];
									INSERT	Monitoreo.dbo.tbl_Catalogo_Indices_Fragmentacion
									SELECT	@@SERVERNAME,
											DB_NAME(),
											SCHEMA_NAME(B.schema_id)	+
											''.''						+
											B.name,
											A.index_id,
											ISNULL(A.name, ''N/A''),
											ISNULL(C.partition_number, 1),
											F.alloc_unit_type_desc,
											E.Tamano,
											A.fill_factor,
											CAST(F.avg_fragmentation_in_percent AS NUMERIC(5,2)),
											getdate()
									FROM	[sys].[indexes]		AS	A
																INNER JOIN 
											[sys].[objects]		AS	B		ON	B.object_id =	A.object_id
																LEFT JOIN  
											[sys].[partitions]	AS	C		ON	C.object_id =	A.object_id		AND 
																				C.index_id  =	A.index_id
																LEFT JOIN 
											(SELECT	A1.object_id,
													A1.index_id,
													SUM(D.used_pages) * 8 AS Tamano
											FROM    [sys].[indexes]		A1
																		INNER JOIN 
													[sys].[partitions]	C1		ON	C1.object_id =	A1.object_id	AND 
																					C1.index_id  =	A1.index_id
																		INNER JOIN 
													[sys].[allocation_units] D	ON	D.container_id	=	C1.partition_id
											GROUP BY A1.[object_id], 
													 A1.[index_id]) E	ON	E.[object_id]	=	A.[object_id]		AND 
																			E.[index_id]		=	A.[index_id]
															LEFT JOIN 
											[sys].[dm_db_index_physical_stats] (DB_ID(),
																				NULL,
																				NULL,
																				NULL,
																				''LIMITED'')	as	F			on	F.[object_id]			=	A.[object_id]		AND 
																													F.[index_id]			=	A.[index_id]		AND 
																													F.[database_id]			=	DB_ID()
									WHERE	B.[type]			IN	(''U'',''V'')	AND
											B.[is_ms_shipped]	=	0x0				AND
											A.[is_disabled]		=	0x0'

print	@COMANDO

EXECUTE master.sys.sp_MSforeachdb	@comando

end
