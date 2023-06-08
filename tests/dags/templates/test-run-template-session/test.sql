drop table if exists tmp_table_data_3;

select {{ti.xcom_pull(key="session").session_id}} as session_id, *
into tmp_table_data_3
from public.table_data;
