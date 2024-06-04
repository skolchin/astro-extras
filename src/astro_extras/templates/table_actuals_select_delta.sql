-- SQL template to select delta for transfer to actuals
select distinct on (t.id) t.*
from {{source_table}} t 
inner join public.sessions s on t.session_id = s.session_id and s.status='success'
{% raw -%}
    and date_trunc('day', s.period[1]) = date_trunc('day', '{{ti.xcom_pull(key="session").period_start}}'::timestamp)
    and date_trunc('day', s.period[2]) = date_trunc('day', '{{ti.xcom_pull(key="session").period_end}}'::timestamp)
{% endraw -%}
order by t.id, t.session_id desc