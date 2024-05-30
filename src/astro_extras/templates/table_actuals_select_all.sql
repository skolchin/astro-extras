-- SQL template to select all data to transfer to actuals
select distinct on (t.id) t.*
from {{source_table}} t 
inner join public.sessions s on t.session_id = s.session_id and s.status='success'
order by t.id, t.session_id desc