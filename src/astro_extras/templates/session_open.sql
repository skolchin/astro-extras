-- Open new session
insert into public.sessions(source, target, period, started, status, run_id) 
values(
    '{{ source_conn_id }}',
    '{{ destination_conn_id }}',
    '{{ period_str }}', 
    '{{ started }}',
    'running',
    '{{ run_id }}'
) returning session_id