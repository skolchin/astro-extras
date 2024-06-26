-- SQL template to select data for ods-to-actuals transfer
select distinct on (t.id) t.*
from {{source_table}} t inner join public.sessions s
{% raw -%}
    on t.session_id = s.session_id and s.status='success'
    and date_trunc('day', s.period[1]) = date_trunc('day', '{{ti.xcom_pull(key="session").period_start}}'::timestamp)
    and date_trunc('day', s.period[2]) = date_trunc('day', '{{ti.xcom_pull(key="session").period_end}}'::timestamp)
{% endraw -%}
    order by t.id, t.session_id desc