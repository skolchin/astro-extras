# Example on how to query Marquez database to get source-to-target mapping.
#
# Includes SQL query parsing using Langchain with Ollama LLM
# to find formulas which are used to fill in the fields.
#
# Results are stored to marques_meta.xlsx file.

import os
import pandas as pd
from functools import cache
from textwrap import dedent
from dotenv import load_dotenv
from sqlalchemy import create_engine

try:
    from httpx import ConnectError as OllamaConnectError        # pyright: ignore[reportMissingImports]
except ModuleNotFoundError:
    class OllamaConnectError(Exception): pass

TABLE_FIELDS_SQL = dedent(
"""
    select distinct on (table_id, field_id)
        df.dataset_uuid as table_id,
        df.uuid as field_id,
        df.created_at,
        d.name as table_name,
        d.description as table_comment,
        df.name as field_name,
        df.type as field_type,
        df.description as field_comment
    from dataset_fields df
    inner join datasets d on df.dataset_uuid = d.uuid
    order by table_id, field_id, created_at desc
""")

JOBS_SQL = dedent(
"""
    select distinct on (job_id)
        r.uuid as run_id,
        j.uuid as job_id,
        rf.facet #>> '{airflow,dag,dag_id}' as dag,
    	rf.facet #>> '{airflow,dag,schedule_interval}' as schedule,
        r.started_at,
        r.ended_at,
        r.current_run_state,
        r.job_name as task,
        null as source_conn_id,
        d.uuid as source_table_id,
        d.name as source_table,
        null as target_conn_id,
        trg.uuid as target_table_id,
        trg.name as target_table,
        jf.facet #>> '{sql,query}' as query
    from jobs j
    inner join job_versions_io_mapping jm on jm.job_uuid = j.uuid and jm.io_type = 'INPUT'
    inner join datasets d on jm.dataset_uuid = d.uuid 
    inner join runs r on j.current_version_uuid = r.job_version_uuid 
    left join job_facets jf on jf.job_uuid = j.uuid and jf.name = 'sql' 
    left join run_facets rf on rf.run_uuid = r.uuid and rf.name = 'airflow' and rf.lineage_event_type = 'COMPLETE'
    left join lateral (
        select d2.uuid, d2.name
        from job_versions_io_mapping jm2
        inner join datasets d2 on jm2.dataset_uuid = d2.uuid
        where jm2.job_uuid = j.uuid and jm2.io_type = 'OUTPUT'
    ) trg on true
    order by job_id, dag, task, started_at desc
""")

QUERY_PARSE_TEMPLATE = dedent(
"""
    statement: {st}
    answer: Analyze SQL statement and provide a list of selected fields
            and a list of tables used by the query.
            For each field indicate which table this field is selected from.
            If a field is calculated by some formula, return the formula as well.
            Answer must be provided as JSON list in the following format:
                {{
                    "fields": [
                        {{
                            "field_name": "xxx",
                            "table_name": "table",
                            "table_alias": "t"
                            "formula": "t.x + 1"
                        }}
                    ],
                    "tables": [
                        {{
                            "table_name": "table",
                            "table_alias": "t"
                        }}
                    ]
                }}
            Do not output explanations.
""")

# ollama pull qwen2.5
MODEL_NAME = 'qwen2.5'

@cache
def get_llm():
    from langchain_ollama.llms import OllamaLLM         # pyright: ignore[reportMissingImports]
    from langchain_core.prompts import PromptTemplate   # pyright: ignore[reportMissingImports]
    from langchain_core.output_parsers import JsonOutputParser  # pyright: ignore[reportMissingImports]

    llm = OllamaLLM(model=MODEL_NAME, temperature=0.1)
    prompt = PromptTemplate.from_template(QUERY_PARSE_TEMPLATE)
    return prompt | llm | JsonOutputParser()

def parse_sql(sql: str) -> dict:
    return get_llm().invoke({'st': sql})

load_dotenv()

marquez_url = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@localhost/marquez'

# detect LLM is here
try:
    parse_sql('select * from xxx')
    query_parsing_available = True
except ModuleNotFoundError:
    query_parsing_available = False
    print('To parse SQL queries, install Ollama LLM by running `pip install langchain langchain_ollama`')
except OllamaConnectError:
    query_parsing_available = False
    print('Cannot connect to Ollama LLM service, try to start it with `sudo systemctl start ollama`')

# get list of datasets and jobs
with create_engine(marquez_url).connect() as conn:
    jobs: pd.DataFrame = pd.read_sql(JOBS_SQL, conn)
    fields: pd.DataFrame = pd.read_sql(TABLE_FIELDS_SQL, conn)

with pd.ExcelWriter('./marques_meta.xlsx') as xls:
    # Summary sheet
    jobs['source_conn_id'] = jobs['source_table'].map(lambda x: x.split('.')[0])
    jobs['source_table'] = jobs['source_table'].map(lambda x: '.'.join(x.split('.')[1:]))
    jobs['target_conn_id'] = jobs['target_table'].map(lambda x: x.split('.')[0])
    jobs['target_table'] = jobs['target_table'].map(lambda x: '.'.join(x.split('.')[1:]))
    jobs.drop(columns=['run_id', 'job_id', 'source_table_id', 'target_table_id']) \
        .sort_values(['dag', 'task']) \
        .to_excel(xls, sheet_name='summary', index=False)

    # Separate sheet for every table mapping
    for _, row in jobs.iterrows():
        src_table = row['source_table']
        trg_table = row['target_table']

        if query_parsing_available:
            field_map = parse_sql(row['query'])
            table_field_formulas = {f"{v['table_name']}.{v['field_name']}": v.get('formula') \
                                    for v in field_map['fields'] if v.get('table_name')}
            field_formulas = {f"{v['field_name']}": v.get('formula') \
                              for v in field_map['fields'] if not v.get('table_name')}
        else:
            field_map = {}

        src_fields = fields.query('table_id==@row["source_table_id"]').copy()
        src_fields['table_name'] = src_fields['table_name'].map(lambda x: '.'.join(x.split('.')[1:]))
        trg_fields = fields.query('table_id==@row["target_table_id"]').copy()
        trg_fields['table_name'] = trg_fields['table_name'].map(lambda x: '.'.join(x.split('.')[1:]))

        merged_fields = src_fields[['table_name', 'field_name', 'field_type', 'field_comment']] \
                            .merge(trg_fields[['table_name', 'field_name', 'field_type', 'field_comment']], 
                                   how='outer', 
                                   on='field_name', 
                                   suffixes=['_src', '_trg'])
        # merged_fields['field_name'] = merged_fields['field_name'].where(~merged_fields['table_name_src'].isna(), None)

        cols = merged_fields.columns.copy()
        rename_cols = {}
        for col in cols:
            if col.endswith('_trg'):
                rename_cols[col] = ('target', col[:-4])
            elif col.endswith('_src'):
                rename_cols[col] = ('source', col[:-4])
            elif col == 'field_name':
                rename_cols[col] = ('source', col)
                rename_cols[col+'_trg'] = ('target', col)
                merged_fields.insert(len(cols)-2, col+'_trg', merged_fields[col])
        
        merged_fields.rename(columns=rename_cols, inplace=True)
        if field_map:
            full_names = merged_fields.apply(lambda r: '.'.join(r[('target', 'table_name')].split('.')[1:]) + '.' + \
                                                                r[('target', 'field_name')], axis=1)
            full_name_formulas = full_names.map(table_field_formulas)
            merged_fields.insert(4, ('source', 'formula'), full_name_formulas)

            single_name_formulas = merged_fields[('target', 'field_name')].map(field_formulas)
            merged_fields[('source', 'formula')].fillna(single_name_formulas, inplace=True)

        merged_fields.columns = pd.MultiIndex.from_tuples(merged_fields.columns)
        merged_fields.to_excel(xls, sheet_name=row['task'], index=True)
