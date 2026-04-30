\set ON_ERROR_STOP on

\if :{?scale}
\else
\set scale 1
\endif

\if :{?use_brin}
\else
\set use_brin false
\endif

\if :{?single_key_cluster}
\else
\set single_key_cluster false
\endif

\if :{?text_cluster_key}
\else
\set text_cluster_key false
\endif

\if :{?hot_tile_fraction}
\else
\set hot_tile_fraction 0
\endif

\timing on

drop table if exists clustered_write_osm_diff cascade;
drop table if exists clustered_write_osm_diff_on cascade;
drop table if exists clustered_write_osm_diff_off cascade;

create temp table clustered_write_settings as
select (200000 * :scale)::int as base_rows,
       (20000 * :scale)::int as insert_rows,
       (20000 * :scale)::int as update_rows,
       (4096 * :scale)::int as tile_count,
       (:'use_brin')::boolean as brin_enabled,
       (:'single_key_cluster')::boolean as single_key_cluster,
       (:'text_cluster_key')::boolean as text_cluster_key,
       (:hot_tile_fraction)::numeric as hot_tile_fraction;

create unlogged table clustered_write_osm_diff_on
(
    osm_id bigint primary key,
    tile_id int not null,
    cluster_key text generated always as ('g' || lpad(tile_id::text, 8, '0')) stored,
    version int not null,
    payload text not null
) with (fillfactor = 90);

insert into clustered_write_osm_diff_on (osm_id, tile_id, version, payload)
select g,
       ((g - 1) % s.tile_count) + 1,
       1,
       repeat('base', 16)
from clustered_write_settings as s,
     generate_series(1, s.base_rows) as g;

\if :text_cluster_key
create index clustered_write_osm_diff_tile_idx
    on clustered_write_osm_diff_on (cluster_key);
\else
    \if :single_key_cluster
create index clustered_write_osm_diff_tile_idx
    on clustered_write_osm_diff_on (tile_id);
    \else
create index clustered_write_osm_diff_tile_idx
    on clustered_write_osm_diff_on (tile_id, osm_id);
    \endif
\endif

\if :use_brin
\if :text_cluster_key
create index clustered_write_osm_diff_tile_brin_idx
    on clustered_write_osm_diff_on using brin (cluster_key) with (pages_per_range = 32);
\else
create index clustered_write_osm_diff_tile_brin_idx
    on clustered_write_osm_diff_on using brin (tile_id) with (pages_per_range = 32);
\endif
\endif

cluster clustered_write_osm_diff_on using clustered_write_osm_diff_tile_idx;
analyze clustered_write_osm_diff_on;

create unlogged table clustered_write_osm_diff_off
(
    osm_id bigint primary key,
    tile_id int not null,
    cluster_key text generated always as ('g' || lpad(tile_id::text, 8, '0')) stored,
    version int not null,
    payload text not null
) with (fillfactor = 90);

insert into clustered_write_osm_diff_off (osm_id, tile_id, version, payload)
select osm_id,
       tile_id,
       version,
       payload
from clustered_write_osm_diff_on;

\if :text_cluster_key
create index clustered_write_osm_diff_off_tile_idx
    on clustered_write_osm_diff_off (cluster_key);
\else
    \if :single_key_cluster
create index clustered_write_osm_diff_off_tile_idx
    on clustered_write_osm_diff_off (tile_id);
    \else
create index clustered_write_osm_diff_off_tile_idx
    on clustered_write_osm_diff_off (tile_id, osm_id);
    \endif
\endif

\if :use_brin
\if :text_cluster_key
create index clustered_write_osm_diff_off_tile_brin_idx
    on clustered_write_osm_diff_off using brin (cluster_key) with (pages_per_range = 32);
\else
create index clustered_write_osm_diff_off_tile_brin_idx
    on clustered_write_osm_diff_off using brin (tile_id) with (pages_per_range = 32);
\endif
\endif

cluster clustered_write_osm_diff_off using clustered_write_osm_diff_off_tile_idx;
alter table clustered_write_osm_diff_off set without cluster;
analyze clustered_write_osm_diff_off;

-- Start the diff phase from a fresh relcache.  The control table was just
-- clustered and then marked SET WITHOUT CLUSTER; reconnecting ensures the
-- following writes observe the cleared catalog bit rather than any relation
-- state cached during setup.
select current_database() as clustered_write_database \gset
\connect :clustered_write_database
\timing on

create function pg_temp.tid_block(tid)
returns bigint
language sql
immutable
parallel safe
as $$
    select split_part(trim(both '()' from $1::text), ',', 1)::bigint;
$$;

create temp table clustered_write_settings as
select (200000 * :scale)::int as base_rows,
       (20000 * :scale)::int as insert_rows,
       (20000 * :scale)::int as update_rows,
       (4096 * :scale)::int as tile_count,
       (:'use_brin')::boolean as brin_enabled,
       (:'single_key_cluster')::boolean as single_key_cluster,
       (:'text_cluster_key')::boolean as text_cluster_key,
       (:hot_tile_fraction)::numeric as hot_tile_fraction;

create temp table clustered_write_step_timings
(
    step text primary key,
    started_at timestamptz not null,
    finished_at timestamptz
);

create temp table clustered_write_base_ranges as
select 'clustered_write'::text as variant,
       tile_id,
       min(pg_temp.tid_block(ctid)) as min_block,
       max(pg_temp.tid_block(ctid)) as max_block
from clustered_write_osm_diff_on
group by tile_id

union all

select 'without_cluster_metadata'::text as variant,
       tile_id,
       min(pg_temp.tid_block(ctid)) as min_block,
       max(pg_temp.tid_block(ctid)) as max_block
from clustered_write_osm_diff_off
group by tile_id;

create temp table clustered_write_diff_inserts as
select s.base_rows + g as osm_id,
       (g <= (s.insert_rows * s.hot_tile_fraction)::int) as is_hot_insert,
       case
         when g <= (s.insert_rows * s.hot_tile_fraction)::int then 1
         else (((g::bigint * 1103515245 + 12345) % s.tile_count) + 1)::int
       end as tile_id
from clustered_write_settings as s,
     generate_series(1, s.insert_rows) as g;

insert into clustered_write_step_timings
values ('clustered_write_insert', clock_timestamp(), null);

insert into clustered_write_osm_diff_on (osm_id, tile_id, version, payload)
select d.osm_id,
       d.tile_id,
       1,
       repeat('insert', 16)
from clustered_write_diff_inserts as d;

update clustered_write_step_timings
set finished_at = clock_timestamp()
where step = 'clustered_write_insert';

insert into clustered_write_step_timings
values ('without_cluster_metadata_insert', clock_timestamp(), null);

insert into clustered_write_osm_diff_off (osm_id, tile_id, version, payload)
select d.osm_id,
       d.tile_id,
       1,
       repeat('insert', 16)
from clustered_write_diff_inserts as d;

update clustered_write_step_timings
set finished_at = clock_timestamp()
where step = 'without_cluster_metadata_insert';

create temp table clustered_write_diff_updates as
select distinct (((g::bigint * 2654435761) % s.base_rows) + 1)::bigint as osm_id
from clustered_write_settings as s,
     generate_series(1, s.update_rows) as g;

insert into clustered_write_step_timings
values ('clustered_write_update', clock_timestamp(), null);

update clustered_write_osm_diff_on as o
set version = o.version + 1,
    payload = repeat('updated-row', 64)
from clustered_write_diff_updates as u
where o.osm_id = u.osm_id;

update clustered_write_step_timings
set finished_at = clock_timestamp()
where step = 'clustered_write_update';

insert into clustered_write_step_timings
values ('without_cluster_metadata_update', clock_timestamp(), null);

update clustered_write_osm_diff_off as o
set version = o.version + 1,
    payload = repeat('updated-row', 64)
from clustered_write_diff_updates as u
where o.osm_id = u.osm_id;

update clustered_write_step_timings
set finished_at = clock_timestamp()
where step = 'without_cluster_metadata_update';

analyze clustered_write_osm_diff_on;
analyze clustered_write_osm_diff_off;

select s.brin_enabled,
       s.text_cluster_key,
       t.step,
       round((extract(epoch from t.finished_at - t.started_at) * 1000)::numeric, 2) as elapsed_ms
from clustered_write_step_timings as t
join clustered_write_settings as s on true
order by t.step;

with measured as
(
    select 'clustered_write'::text as variant,
           case
             when s.hot_tile_fraction > 0 and d.is_hot_insert then 'insert_hot'
             when s.hot_tile_fraction > 0 then 'insert_rest'
             else 'insert'
           end as diff_kind,
           pg_temp.tid_block(o.ctid) as heap_block,
           r.min_block,
           r.max_block
    from clustered_write_osm_diff_on as o
    join clustered_write_settings as s on true
    join clustered_write_diff_inserts as d
      on d.osm_id = o.osm_id
    join clustered_write_base_ranges as r
      on r.variant = 'clustered_write'
     and r.tile_id = o.tile_id
    where o.osm_id > s.base_rows

    union all

    select 'clustered_write'::text as variant,
           'update'::text as diff_kind,
           pg_temp.tid_block(o.ctid) as heap_block,
           r.min_block,
           r.max_block
    from clustered_write_osm_diff_on as o
    join clustered_write_settings as s on true
    join clustered_write_base_ranges as r
      on r.variant = 'clustered_write'
     and r.tile_id = o.tile_id
    where o.osm_id <= s.base_rows
      and o.version > 1

    union all

    select 'without_cluster_metadata'::text as variant,
           case
             when s.hot_tile_fraction > 0 and d.is_hot_insert then 'insert_hot'
             when s.hot_tile_fraction > 0 then 'insert_rest'
             else 'insert'
           end as diff_kind,
           pg_temp.tid_block(o.ctid) as heap_block,
           r.min_block,
           r.max_block
    from clustered_write_osm_diff_off as o
    join clustered_write_settings as s on true
    join clustered_write_diff_inserts as d
      on d.osm_id = o.osm_id
    join clustered_write_base_ranges as r
      on r.variant = 'without_cluster_metadata'
     and r.tile_id = o.tile_id
    where o.osm_id > s.base_rows

    union all

    select 'without_cluster_metadata'::text as variant,
           'update'::text as diff_kind,
           pg_temp.tid_block(o.ctid) as heap_block,
           r.min_block,
           r.max_block
    from clustered_write_osm_diff_off as o
    join clustered_write_settings as s on true
    join clustered_write_base_ranges as r
      on r.variant = 'without_cluster_metadata'
     and r.tile_id = o.tile_id
    where o.osm_id <= s.base_rows
      and o.version > 1
),
drift as
(
    select variant,
           diff_kind,
           heap_block,
           case
             when heap_block < min_block then min_block - heap_block
             when heap_block > max_block then heap_block - max_block
             else 0
           end as block_drift
    from measured
)
select s.brin_enabled,
       s.text_cluster_key,
       variant,
       diff_kind,
       count(*) as rows_measured,
       count(distinct heap_block) as heap_blocks_touched,
       round(100.0 * avg((block_drift = 0)::int), 2) as pct_inside_base_range,
       round(avg(block_drift)::numeric, 2) as avg_block_drift,
       round((percentile_cont(0.95) within group (order by block_drift))::numeric, 2) as p95_block_drift,
       max(block_drift) as max_block_drift
from drift
join clustered_write_settings as s on true
group by s.brin_enabled, s.text_cluster_key, variant, diff_kind
order by s.brin_enabled, s.text_cluster_key, variant, diff_kind;
