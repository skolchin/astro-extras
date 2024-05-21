SELECT 
    {{ ti.xcom_pull(key="session").session_id }} AS session_id,
    id as id,
    test1 AS test1,
    test2 AS test2,
    test3 AS test3,
    test4 AS test4,
    mod_ts AS mod_ts
FROM public.test_table_4
WHERE mod_ts BETWEEN CAST('{{ ti.xcom_pull(key="session").period_start }}' AS timestamp) 
                AND CAST('{{ ti.xcom_pull(key="session").period_end }}' AS timestamp) and id between 10 and 20;
