select 
    {{ti.xcom_pull(key="session").session_id}} as session_id,
    id,
    type_id,
    comments,
    created_ts as created_ts,
    modified_ts as modified_ts
from table_data 
where created_ts between TO_TIMESTAMP_TZ('{{ti.xcom_pull(key="session").period_start}}', 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM')
        and TO_TIMESTAMP_TZ('{{ti.xcom_pull(key="session").period_end}}', 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM') 
    or modified_ts between TO_TIMESTAMP_TZ('{{ti.xcom_pull(key="session").period_start}}', 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM')
        and TO_DATE('{{ti.xcom_pull(key="session").period_end}}', 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM') 
