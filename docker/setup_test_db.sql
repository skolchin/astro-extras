--
-- Airflow DB setup
--
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

create table public.test_table_1(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_2(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_3(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_4(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_5(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_6(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_7(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

insert into public.test_table_1(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

insert into public.test_table_2(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

insert into public.test_table_3(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

insert into public.test_table_4(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

insert into public.test_table_5(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

insert into public.test_table_6(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

insert into public.test_table_7(test1, test2, test3, test4)
select md5(random()::text), (random()*32767)::int, random(), random()>=0.5
from generate_series(1, 100) i;

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

create table public.test_table_1(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_2(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_3(
    id serial not null primary key,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_4(
    session_id int not null references public.sessions(session_id),
    id int not null,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_5(
    session_id int not null references public.sessions(session_id),
    id int not null,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_6(
    session_id int not null references public.sessions(session_id),
    id int not null,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create table public.test_table_7(
    session_id int not null references public.sessions(session_id),
    _modified timestamptz,
    _deleted timestamptz,
    id int not null,
    test1 text not null,
    test2 int not null,
    test3 float not null,
    test4 bool not null
);

create view public.test_table_4_a as
select * from public.test_table_4
where session_id = (select max(session_id) from public.sessions where status = 'success');

create or replace view public.test_table_7_a as
with d as (
    select distinct on (r.id) r.id, r.session_id, r._deleted from public.sessions s
    inner join public.test_table_7 r on r.session_id = s.session_id and s.status = 'success'
    order by r.id, r.session_id desc)
select t.* from public.test_table_7 t inner join d on t.id = d.id and t.session_id = d.session_id and d._deleted is null;
