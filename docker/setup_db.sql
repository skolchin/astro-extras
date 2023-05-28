create database airflow;
create database source_db;
create database target_db;

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
	 ('target_db','postgres','','postgres','target_db','postgres','918ja620_82',5432,false,false,''),
	 ('source_db','postgres','','postgres','source_db','postgres','918ja620_82',5432,false,false,'');

\c source_db

create table public.dict_types(
    type_id serial not null primary key,
    type_name text not null
);

create table public.table_data(
    id serial not null primary key,
    type_id int not null references dict_types(type_id),
    comments text not null,
    created_ts timestamp not null default current_timestamp,
    modified_ts timestamp null
);

insert into public.dict_types(type_id, type_name)
values (1, 'Type 1'), (2, 'Type 2'), (3, 'Type 3');

insert into public.table_data(type_id, comments)
select floor(random()*3) + 1, md5(random()::text)
from generate_series(1, 1000);

\c target_db

create schema stage;

create table public.sessions(
    session_id serial not null primary key,
    source text not null,
    target text not null,
    period timestamptz[2] not null,
    run_id text,
    started timestamptz not null default current_timestamp,
    finished timestamptz,
    status varchar(10) not null default 'running' check (status in ('running', 'success', 'error'))
);

create table stage.dict_types(
    session_id int not null references public.sessions(session_id),
    type_id int not null,
    type_name text not null
);

create table stage.table_data(
    session_id int not null references public.sessions(session_id),
    id int not null,
    type_id int not null,
    comments text not null,
    created_ts timestamptz not null default current_timestamp,
    modified_ts timestamptz null
);
