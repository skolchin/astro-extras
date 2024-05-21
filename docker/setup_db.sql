--
-- Airflow DB setup
--
create database airflow;
comment on database airflow is 'Internal Airflow database';

\c airflow

create table public."connection" (
	id serial not null primary key,
	conn_id varchar(250) not null unique,
	conn_type varchar(500) not null,
	description text null,
	host varchar(500) null,
	"schema" varchar(500) null,
	login varchar(500) null,
	"password" varchar(5000) null,
	port int4 null,
	is_encrypted bool null,
	is_extra_encrypted bool null,
	extra text null
);

insert into public."connection" (conn_id,conn_type,description,host,"schema",login,"password",port,is_encrypted,is_extra_encrypted,extra) 
values
	('source_db','postgres','','postgres','source_db','postgres','918ja620_82',5432,false,false,''),
	('target_db','postgres','','postgres','target_db','postgres','918ja620_82',5432,false,false,''),
	('stage_db','postgres','','postgres','target_db','postgres','918ja620_82',5432,false,false,''),
	('actuals_db','postgres','','postgres','actuals_db','postgres','918ja620_82',5432,false,false,''),
	('dwh_db','postgres','','postgres','target_db','postgres','918ja620_82',5432,false,false,'');

--
-- Marquez DB setup
--
-- create user marquez with encrypted password 'marquez';
-- create database marquez with owner marquez;
-- comment on database marquez is 'Internal Marquez database';

--
-- Source DB setup
--
create database source_db;
comment on database source_db is 'Source database';

\c source_db

create table public.types(
    type_id serial not null primary key,
    type_name text not null
);
comment on table public.types is 'Type dictionary table';

create table public.table_data(
    id serial not null primary key,
    type_id int not null references types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table public.table_data is 'Data table for standard style transfer';

create table public.ods_data(
    id serial not null primary key,
    type_id int not null references types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table public.ods_data is 'Data table for ODS-style transfer';

insert into public.types(type_id, type_name)
values (1, 'Type 1'), (2, 'Type 2'), (3, 'Type 3');

insert into public.table_data(type_id, comments, created_ts)
select floor(random()*3) + 1, md5(random()::text), current_timestamp - '1 day'::interval
from generate_series(1, 1000);

insert into public.ods_data(type_id, comments, created_ts)
select floor(random()*3) + 1, md5(random()::text), current_timestamp - '1 day'::interval
from generate_series(1, 1000);

--
-- Target DB setup
-- Target DB contains multiple schemas: 
--  stage for source data snapshots
--  actuals which merges snapshots to actual source data
--  dwh which is supposed to contain some data marts and cubes

create database target_db;
comment on database target_db is 'Target (stage) database';

\c target_db

create table public.sessions(
    session_id serial not null primary key,
    source text not null,
    target text not null,
    period timestamp[2] not null,
    run_id text,
    started timestamp not null default current_timestamp,
    finished timestamp,
    status varchar(10) not null default 'running' check (status in ('running', 'success', 'error'))
);
comment on table public.sessions is 'Sessions';

-- stage
create schema stage;
comment on schema stage is 'Staging area';

create table stage.types(
    session_id int not null references public.sessions(session_id),
    type_id int not null,
    type_name text not null
);
comment on table stage.types is 'Staged types table';

create view stage.types_a as
    select distinct on (t.type_id) t.*
    from stage.types t
    inner join public.sessions s on s.session_id = t.session_id and s.status = 'success'
    order by t.type_id, t.session_id desc;

comment on view stage.types_a is 'Actual data view for stage.types';

create table stage.table_data(
    session_id int not null references public.sessions(session_id),
    id int not null,
    type_id int not null,
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table stage.table_data is 'Staged data table';

create view stage.table_data_a as
    select distinct on (t.id) t.*
    from stage.table_data t
    inner join public.sessions s on s.session_id = t.session_id and s.status = 'success'
    order by t.id, t.session_id desc;

comment on view stage.table_data_a is 'Actual data view for stage.table_data';

create table stage.ods_data (
    session_id int not null references public.sessions(session_id),
    _modified timestamp,
    _deleted timestamp,
    id int not null,
    type_id int not null,
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table stage.ods_data is 'Staged ODS data table';

create view stage.ods_data_a as
    with d as (
        select distinct on (r.id) r.id, r.session_id, r._deleted from public.sessions s
        inner join stage.ods_data r on r.session_id = s.session_id and s.status = 'success'
        order by r.id, r.session_id desc
    )
    select t.* from stage.ods_data t inner join d on t.id = d.id and t.session_id = d.session_id and d._deleted is null;

comment on view stage.ods_data_a is 'Actual data view for stage.ods_data';

-- actuals
create schema actuals;
comment on schema actuals is 'Actuals area (for same-db transfers)';

create table actuals.types(
    type_id int not null primary key,
    session_id int not null,
    type_name text not null
);
comment on table actuals.types is 'Actuals types table';

create table actuals.table_data(
    id int not null primary key,
    session_id int not null,
    type_id int not null references actuals.types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table actuals.table_data is 'Actuals data table';

create table actuals.ods_data (
    id int not null primary key,
    session_id int not null,
    _modified timestamp,
    _deleted timestamp,
    type_id int not null references actuals.types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table actuals.ods_data is 'Actuals ODS data table';

create view actuals.ods_data_a as select * from actuals.ods_data where "_deleted" is null order by id;
comment on view actuals.ods_data_a is 'Actual data view for Actuals ODS data table';

-- dwh
create schema dwh;
comment on schema dwh is 'DWH data area';

create table dwh.dim_types(
    type_id serial not null primary key,
    session_id int not null,
    effective_from timestamp not null default current_timestamp,
    effective_to timestamp null,
    src_id int not null,
    type_name text not null
);
comment on table dwh.dim_types is 'Time-dependent types dictionary';

create view dwh.dim_types_a as 
    select * from dwh.dim_types
    where effective_from <= current_timestamp and 
          (effective_to is null or effective_to > current_timestamp);

comment on view dwh.dim_types_a is 'Time-dependent types dictionary (actual data)';

create table dwh.data_facts(
    fact_id serial not null primary key,
    fact_dttm timestamp not null default current_timestamp,
    session_id int not null,
    type_id int not null references dwh.dim_types(type_id),
    num_records int not null
);
comment on table dwh.data_facts is 'Fact table';

create unique index data_facts_uq_idx on dwh.data_facts(type_id);

--
-- Actuals DB setup

create database actuals_db;
comment on database actuals_db is 'Actuals database (for different db transfers)';

\c actuals_db

-- actuals
create schema actuals;
comment on schema actuals is 'Actuals area';

create table actuals.types(
    type_id int not null primary key,
    session_id int not null,
    type_name text not null
);
comment on table actuals.types is 'Actuals types table';

create table actuals.table_data(
    id int not null primary key,
    session_id int not null,
    type_id int not null references actuals.types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table actuals.table_data is 'Actuals data table';

create table actuals.ods_data (
    id int not null primary key,
    session_id int not null,
    _modified timestamp,
    _deleted timestamp,
    type_id int not null references actuals.types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);
comment on table actuals.ods_data is 'Actuals ODS data table';

create view actuals.ods_data_a as select * from actuals.ods_data where "_deleted" is null order by id;
comment on view actuals.ods_data_a is 'Actual data view for Actuals ODS data table';
