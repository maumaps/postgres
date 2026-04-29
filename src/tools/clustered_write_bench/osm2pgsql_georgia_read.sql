\set ON_ERROR_STOP on
\timing on

create extension if not exists postgis;

drop table if exists clustered_write_read_windows;

create temp table clustered_write_read_windows
(
    name text primary key,
    geom geometry(Polygon, 3857) not null
);

insert into clustered_write_read_windows
values
    ('tbilisi_core', ST_Transform(ST_MakeEnvelope(44.75, 41.67, 44.86, 41.75, 4326), 3857)),
    ('batumi_core', ST_Transform(ST_MakeEnvelope(41.59, 41.61, 41.68, 41.67, 4326), 3857)),
    ('kutaisi_core', ST_Transform(ST_MakeEnvelope(42.62, 42.23, 42.75, 42.31, 4326), 3857)),
    ('georgia_west_east', ST_Transform(ST_MakeEnvelope(39.9, 41.0, 46.8, 43.7, 4326), 3857));

analyze clustered_write_read_windows;

\echo 'bbox count reads'

explain (analyze, buffers, timing, summary)
select w.name, count(*) as rows_seen
from clustered_write_read_windows as w
join planet_osm_point as p
  on p.way && w.geom
group by w.name
order by w.name;

explain (analyze, buffers, timing, summary)
select w.name, count(*) as rows_seen
from clustered_write_read_windows as w
join planet_osm_line as l
  on l.way && w.geom
group by w.name
order by w.name;

explain (analyze, buffers, timing, summary)
select w.name, count(*) as rows_seen
from clustered_write_read_windows as w
join planet_osm_polygon as p
  on p.way && w.geom
group by w.name
order by w.name;

\echo 'exact spatial reads'

explain (analyze, buffers, timing, summary)
select w.name, count(*) as rows_seen
from clustered_write_read_windows as w
join planet_osm_roads as r
  on r.way && w.geom
 and ST_Intersects(r.way, w.geom)
group by w.name
order by w.name;

explain (analyze, buffers, timing, summary)
select w.name, count(*) as rows_seen
from clustered_write_read_windows as w
join planet_osm_polygon as p
  on p.way && w.geom
 and ST_Intersects(p.way, w.geom)
group by w.name
order by w.name;

\echo 'heap locality summary'

with table_blocks as
(
    select 'point'::text as table_name,
           (ctid::text::point)[0]::bigint as block_id,
           ST_GeoHash(ST_Transform(ST_Envelope(way), 4326), 7) as geohash
    from planet_osm_point
    where way is not null

    union all

    select 'line'::text as table_name,
           (ctid::text::point)[0]::bigint as block_id,
           ST_GeoHash(ST_Transform(ST_Envelope(way), 4326), 7) as geohash
    from planet_osm_line
    where way is not null

    union all

    select 'polygon'::text as table_name,
           (ctid::text::point)[0]::bigint as block_id,
           ST_GeoHash(ST_Transform(ST_Envelope(way), 4326), 7) as geohash
    from planet_osm_polygon
    where way is not null
),
geohash_blocks as
(
    select table_name,
           geohash,
           count(*) as row_count,
           count(distinct block_id) as block_count,
           max(block_id) - min(block_id) as block_span
    from table_blocks
    group by table_name, geohash
    having count(*) >= 10
)
select table_name,
       count(*) as geohashes,
       percentile_cont(0.50) within group (order by block_count) as p50_blocks,
       percentile_cont(0.95) within group (order by block_count) as p95_blocks,
       percentile_cont(0.50) within group (order by block_span) as p50_span,
       percentile_cont(0.95) within group (order by block_span) as p95_span
from geohash_blocks
group by table_name
order by table_name;
