-- Memory used by each database
SELECT	"DB Name"		=	DB_NAME(database_id),
		'MB Used'		=	COUNT (1) * 8 / 1024
FROM sys.dm_os_buffer_descriptors
GROUP BY database_id
ORDER BY COUNT (*) * 8 / 1024 DESC
GO
