--
--get all databases sizes
--written by: angel rapallo on 04/25/2019
--
--f.size is the number of 8K pages in the database
--so there are f.size * 8 * 1024 bytes in the database
--the rest is pure conversion to mega/gyga/ etc..
--
--there are 1048576 bytes in a megabyte
--there are 1073741824 bytes in a gigabyte
--
--total_bytes-available_bytes = used bytes and / total_bytes gives
--the percentage used of disk
--
select
d.name,
f.name as filetype,
f.physical_name as physicalfile,
f.state_desc as onlinestatus,
f.size * 8.00 * 1024.00 as bytes,    
cast((f.size * 8.00 * 1024.00) / 1048576.00 as numeric (18,2)) as megabytes,
cast((f.size * 8.00 * 1024.00) / 1073741824.00 as numeric(18,2)) as gigabytes,
cast(cast(v.total_bytes - v.available_bytes as float) / cast(v.total_bytes as float) * 100 as numeric(18,2)) used_disk_percent
from 
                sys.master_files f
    inner join  sys.databases d on d.database_id = f.database_id
    cross apply sys.dm_os_volume_stats(f.database_id, f.file_id) v
order by
    d.name