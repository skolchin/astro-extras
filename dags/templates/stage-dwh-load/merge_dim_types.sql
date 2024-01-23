-- Dim merge script consists of two parts: 1st is to close existing
-- records those attribute values changed and 2nd is to insert
-- records which didn't exist in dim before and to add ones which
-- were closed on step 1 with new values
--
-- Not used anymore, replaced with Python's `transfer_table` operator

--
-- Step 1: close existing dict records
--
with src as (
    select type_id as src_id, type_name from stage.types_a
)
update dwh.dim_types dim
set effective_to = '{{ti.xcom_pull(key="session").period_start}}'::timestamp - '1 second'::interval
from src
where src.src_id = dim.src_id and dim.effective_to is null
  and (src.type_name <> dim.type_name);

---
-- Step 2: insert new or changed records
--
with src as (
    select type_id as src_id, type_name from stage.types_a
),
dim as (
    -- we can't just select by effective_to is null as it was closed above
    select distinct on (src_id) src_id, type_name from dwh.dim_types
    order by src_id, effective_from desc
)
insert into dwh.dim_types(
    session_id,
    effective_from,
    effective_to,
    src_id,
    type_name
)
select
    {{ti.xcom_pull(key="session").session_id}} as session_id,
    case
        when dim.src_id is null then '2000-01-01T00:00:00'::timestamp
        else '{{ti.xcom_pull(key="session").period_start}}'::timestamp
    end as effective_from,
    null as effective_to,
    src.src_id, 
    src.type_name 
from src left join dim on src.src_id = dim.src_id
where dim.src_id is null 
    or (dim.src_id is not null and dim.type_name <> src.type_name)
on conflict do nothing;
