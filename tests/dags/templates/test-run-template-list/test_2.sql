drop table if exists tmp_table_data_2;

select *
into tmp_table_data_2
from public.test_table_1;
