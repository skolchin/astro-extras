drop table if exists tmp_table_data_1;

select *
into tmp_table_data_1
from public.test_table_1;
