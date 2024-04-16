select 
    {{ti.xcom_pull(key="session").session_id}} as session_id,
    id,
    type_id,
    comments,
    created_ts as created_ts,
    modified_ts as modified_ts
from table_data 
where created_ts between '{{ti.xcom_pull(key="session").period_start}}'::timestamp
        and '{{ti.xcom_pull(key="session").period_end}}'::timestamp
    or modified_ts between '{{ti.xcom_pull(key="session").period_start}}'::timestamp
        and '{{ti.xcom_pull(key="session").period_end}}'::timestamp
