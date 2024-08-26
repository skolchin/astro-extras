select 
    {{ti.xcom_pull(key="session").session_id}} as session_id,
    id,
    type_id,
    comments,
    created_ts,
    modified_ts
from stage.ods_data_a;