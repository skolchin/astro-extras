-- SQL template to select all data from some table adding session_id to ouput
select {%raw%}{{ti.xcom_pull(key="session").session_id}}{%endraw%} as session_id, t.* from {{source_table}} t