-- Fact update script
-- This is basically GROUP BY on fact table keys calculating aggregates

with src as (
    select * from stage.table_data_a
)
insert into dwh.data_facts(
    session_id,
    fact_dttm,
    type_id,
    num_records
)
select
    {{ti.xcom_pull(key="session").session_id}} as session_id,
    '{{ti.xcom_pull(key="session").period_start}}'::timestamp as fact_dttm,
    t.type_id,
    count(src.id) as num_records
from src left join dwh.dim_types t on t.src_id = src.type_id
group by t.type_id
on conflict (type_id) do update
set
    session_id = excluded.session_id,
    fact_dttm = excluded.fact_dttm,
    num_records = excluded.num_records;
