drop table if exists tmp_table_data;

select *
into tmp_table_data
from public.table_data;
