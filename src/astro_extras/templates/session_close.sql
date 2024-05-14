-- Close session
update public.sessions set finished='{{finished}}', status='{{status}}' where session_id={{session_id}}
