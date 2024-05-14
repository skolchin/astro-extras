-- SQL template to select data for delta transfer
select t.*
from {{source_table}} t 
inner join public.sessions s on 
    t.session_id = s.session_id and s.status='success'
    and s.period && {%raw%}'{"{{ti.xcom_pull(key="session").period_start}}", "{{ti.xcom_pull(key="session").period_end}}"}'{%endraw%}