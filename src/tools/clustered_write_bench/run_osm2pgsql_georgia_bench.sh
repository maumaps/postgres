#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKDIR="${WORKDIR:-$HOME/tmp/clustered-write-osm2pgsql}"
GEORGIA_URL="${GEORGIA_URL:-https://download.geofabrik.de/europe/georgia-latest.osm.pbf}"
PLANET_DAY_STATE_URL="${PLANET_DAY_STATE_URL:-https://planet.openstreetmap.org/replication/day/state.txt}"
OSC_URL="${OSC_URL:-}"

PATCHED_PG_BIN="${PATCHED_PG_BIN:-$ROOT_DIR/tmp_install/usr/local/pgsql/bin}"
BASELINE_PG_BIN="${BASELINE_PG_BIN:-/usr/lib/postgresql/18/bin}"
STOCK_OSM2PGSQL="${STOCK_OSM2PGSQL:-osm2pgsql}"
CLUSTERED_OSM2PGSQL="${CLUSTERED_OSM2PGSQL:-/tmp/osm2pgsql-clustered-import/build/osm2pgsql}"
OSM2PGSQL_STYLE="${OSM2PGSQL_STYLE:-/tmp/osm2pgsql-clustered-import/default.style}"
OSMIUM="${OSMIUM:-osmium}"

POSTGIS_SHARE="${POSTGIS_SHARE:-/usr/share/postgresql/18/extension}"
POSTGIS_LIB="${POSTGIS_LIB:-/usr/lib/postgresql/18/lib}"

PG_PORT_BASE="${PG_PORT_BASE:-55432}"
OSM2PGSQL_CACHE_MB="${OSM2PGSQL_CACHE_MB:-2048}"
OSM2PGSQL_PROCS="${OSM2PGSQL_PROCS:-4}"
BENCH_VARIANTS="${BENCH_VARIANTS:-baseline_stock,patched_stock,patched_clustered_import}"

mkdir -p "$WORKDIR"/{data,logs,pgdata}

log()
{
    printf '[%s] %s\n' "$(date -Is)" "$*" >&2
}

require_executable()
{
    if ! command -v "$1" >/dev/null 2>&1; then
        printf 'missing executable: %s\n' "$1" >&2
        exit 1
    fi
}

require_path_executable()
{
    if [[ ! -x "$1" ]]; then
        printf 'missing executable: %s\n' "$1" >&2
        exit 1
    fi
}

variant_enabled()
{
    local wanted="$1"

    case ",$BENCH_VARIANTS," in
        *,"$wanted",*) return 0 ;;
        *) return 1 ;;
    esac
}

resolve_executable()
{
    local executable="$1"

    if command -v "$executable" >/dev/null 2>&1; then
        command -v "$executable"
    elif [[ -x "$executable" ]]; then
        printf '%s\n' "$executable"
    else
        printf 'missing executable: %s\n' "$executable" >&2
        exit 1
    fi
}

download_if_missing()
{
    local url="$1"
    local out="$2"

    if [[ -s "$out" ]]; then
        log "using existing $out"
        return
    fi

    log "downloading $url"
    curl --fail --location --retry 3 --output "$out".tmp "$url"
    mv "$out".tmp "$out"
}

osc_path_for_sequence()
{
    local seq="$1"
    printf '%09d' "$seq" | sed -E 's#(...)(...)(...)#\1/\2/\3.osc.gz#'
}

resolve_daily_diff_url()
{
    if [[ -n "$OSC_URL" ]]; then
        printf '%s\n' "$OSC_URL"
        return
    fi

    local state_file="$WORKDIR/data/day-state.txt"
    curl --fail --location --retry 3 --output "$state_file".tmp "$PLANET_DAY_STATE_URL"
    mv "$state_file".tmp "$state_file"

    local sequence
    sequence="$(awk -F= '$1 == "sequenceNumber" {print $2}' "$state_file")"
    if [[ -z "$sequence" ]]; then
        printf 'could not read sequenceNumber from %s\n' "$state_file" >&2
        exit 1
    fi

    printf 'https://planet.openstreetmap.org/replication/day/%s\n' "$(osc_path_for_sequence "$sequence")"
}

simplify_daily_diff()
{
    local in_file="$1"
    local out_file="${in_file%.osc.gz}-simplified.osc.gz"
    local tmp_file="$out_file.tmp.osc.gz"

    if [[ -s "$out_file" && "$out_file" -nt "$in_file" ]]; then
        log "using existing simplified diff $out_file"
        printf '%s\n' "$out_file"
        return
    fi

    log "simplifying daily diff for osm2pgsql append"
    rm -f "$tmp_file"
    if ! "$OSMIUM" merge-changes -s -O --output-format=osc.gz -o "$tmp_file" "$in_file"; then
        rm -f "$tmp_file"
        return 1
    fi
    mv "$tmp_file" "$out_file"
    printf '%s\n' "$out_file"
}

copy_postgis_into_install()
{
    local pg_bin="$1"
    local pg_home
    local pg_extension_dir
    local pg_lib_dir
    pg_home="$(cd "$pg_bin/.." && pwd)"

    if [[ "$pg_bin" == /usr/lib/postgresql/* ]]; then
        return
    fi

    mkdir -p "$pg_home/share/extension" "$pg_home/lib"
    pg_extension_dir="$(cd "$pg_home/share/extension" && pwd -P)"
    pg_lib_dir="$(cd "$pg_home/lib" && pwd -P)"

    if [[ "$(cd "$POSTGIS_SHARE" && pwd -P)" != "$pg_extension_dir" ]]; then
        rm -f "$pg_home/share/extension"/postgis*
        rm -f "$pg_home/share/extension"/address_standardizer*
        cp -a "$POSTGIS_SHARE"/postgis* "$pg_home/share/extension/"
        cp -a "$POSTGIS_SHARE"/sql/postgis* "$pg_home/share/extension/" 2>/dev/null || true
        cp -a "$POSTGIS_SHARE"/address_standardizer* "$pg_home/share/extension/" 2>/dev/null || true
        cp -a "$POSTGIS_SHARE"/sql/address_standardizer* "$pg_home/share/extension/" 2>/dev/null || true
    fi
    if [[ "$(cd "$POSTGIS_LIB" && pwd -P)" != "$pg_lib_dir" ]]; then
        rm -f "$pg_home/lib"/postgis-*.so
        rm -f "$pg_home/lib"/postgis_raster-*.so
        cp -a "$POSTGIS_LIB"/postgis-*.so "$pg_home/lib/"
        cp -a "$POSTGIS_LIB"/postgis_raster-*.so "$pg_home/lib/" 2>/dev/null || true
    fi
}

start_server()
{
    local name="$1"
    local pg_bin="$2"
    local port="$3"
    local data_dir="$WORKDIR/pgdata/$name"
    local log_file="$WORKDIR/logs/$name-postgres.log"
    local socket_dir="${TMPDIR:-/tmp}/clustered-write-osm2pgsql-$port"

    rm -rf "$data_dir"
    rm -rf "$socket_dir"
    mkdir -p "$socket_dir"
    "$pg_bin/initdb" -D "$data_dir" >"$WORKDIR/logs/$name-initdb.log"
    cat >>"$data_dir/postgresql.conf" <<EOF
shared_buffers = '1GB'
maintenance_work_mem = '1GB'
work_mem = '64MB'
checkpoint_timeout = '30min'
max_wal_size = '16GB'
fsync = off
synchronous_commit = off
full_page_writes = off
autovacuum = off
EOF
    "$pg_bin/pg_ctl" -D "$data_dir" -l "$log_file" -o "-p $port -k $socket_dir" start
}

stop_server()
{
    local pg_bin="$1"
    local name="$2"
    local port="${3:-}"
    local data_dir="$WORKDIR/pgdata/$name"

    "$pg_bin/pg_ctl" -D "$data_dir" -m fast stop >/dev/null 2>&1 || true
    if [[ -n "$port" ]]; then
        rm -rf "${TMPDIR:-/tmp}/clustered-write-osm2pgsql-$port"
    fi
}

run_psql()
{
    local pg_bin="$1"
    local port="$2"
    local db="$3"
    shift 3

    "$pg_bin/psql" -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$port" -d "$db" "$@"
}

run_variant()
{
    local name="$1"
    local pg_bin="$2"
    local port="$3"
    local osm2pgsql="$4"
    local mode="$5"
    local pbf="$6"
    local osc="$7"

    require_path_executable "$pg_bin/initdb"
    require_path_executable "$pg_bin/pg_ctl"
    require_path_executable "$pg_bin/psql"
    require_path_executable "$osm2pgsql"
    copy_postgis_into_install "$pg_bin"

    log "starting PostgreSQL for $name on port $port"
    start_server "$name" "$pg_bin" "$port"
    trap "stop_server '$pg_bin' '$name' '$port'" EXIT

    "$pg_bin/createdb" -h 127.0.0.1 -p "$port" osm
    run_psql "$pg_bin" "$port" osm -c 'create extension postgis; create extension hstore;'

    local common_args=(
        --slim
        --database osm
        --host 127.0.0.1
        --port "$port"
        --style "$OSM2PGSQL_STYLE"
        --cache "$OSM2PGSQL_CACHE_MB"
        --number-processes "$OSM2PGSQL_PROCS"
    )

    local create_args=(--create)
    if [[ "$mode" == "clustered_import" ]]; then
        create_args+=(--cluster-during-import)
    fi

    log "initial import for $name"
    /usr/bin/time -v -o "$WORKDIR/logs/$name-create.time" \
        "$osm2pgsql" "${common_args[@]}" "${create_args[@]}" "$pbf" \
        >"$WORKDIR/logs/$name-create.stdout" \
        2>"$WORKDIR/logs/$name-create.stderr"

    local append_args=(--append)
    if [[ "$mode" == "clustered_import" ]]; then
        append_args+=(--cluster-during-import)
    fi

    log "daily diff append for $name"
    /usr/bin/time -v -o "$WORKDIR/logs/$name-append.time" \
        "$osm2pgsql" "${common_args[@]}" "${append_args[@]}" "$osc" \
        >"$WORKDIR/logs/$name-append.stdout" \
        2>"$WORKDIR/logs/$name-append.stderr"

    log "read benchmark for $name"
    run_psql "$pg_bin" "$port" osm \
        -f "$BENCH_DIR/osm2pgsql_georgia_read.sql" \
        >"$WORKDIR/logs/$name-read.sqlout" \
        2>"$WORKDIR/logs/$name-read.sqlerr"

    run_psql "$pg_bin" "$port" osm \
        -c "select current_database() as db, pg_size_pretty(pg_database_size(current_database())) as database_size;" \
        >"$WORKDIR/logs/$name-size.sqlout"

    stop_server "$pg_bin" "$name" "$port"
    trap - EXIT
}

require_executable curl
require_executable awk
require_executable sed
require_executable "$OSMIUM"
if variant_enabled baseline_stock; then
    require_path_executable "$BASELINE_PG_BIN/initdb"
fi
if variant_enabled patched_stock || variant_enabled patched_clustered_import; then
    require_path_executable "$PATCHED_PG_BIN/initdb"
fi
if variant_enabled patched_clustered_import; then
    require_path_executable "$CLUSTERED_OSM2PGSQL"
fi
if [[ ! -f "$OSM2PGSQL_STYLE" ]]; then
    printf 'missing osm2pgsql style file: %s\n' "$OSM2PGSQL_STYLE" >&2
    exit 1
fi
stock_osm2pgsql_path=""
if variant_enabled baseline_stock || variant_enabled patched_stock; then
    stock_osm2pgsql_path="$(resolve_executable "$STOCK_OSM2PGSQL")"
fi

georgia_pbf="$WORKDIR/data/georgia-latest.osm.pbf"
daily_diff_url="$(resolve_daily_diff_url)"
daily_diff="$WORKDIR/data/$(basename "$daily_diff_url")"

download_if_missing "$GEORGIA_URL" "$georgia_pbf"
download_if_missing "$daily_diff_url" "$daily_diff"
simplified_daily_diff="$(simplify_daily_diff "$daily_diff")"

log "benchmark inputs:"
log "  georgia=$GEORGIA_URL"
log "  daily_diff=$daily_diff_url"
log "  simplified_daily_diff=$simplified_daily_diff"
log "  variants=$BENCH_VARIANTS"
log "  workdir=$WORKDIR"

if variant_enabled baseline_stock; then
    run_variant baseline_stock "$BASELINE_PG_BIN" "$((PG_PORT_BASE + 1))" "$stock_osm2pgsql_path" stock "$georgia_pbf" "$simplified_daily_diff"
fi

if variant_enabled patched_stock; then
    run_variant patched_stock "$PATCHED_PG_BIN" "$((PG_PORT_BASE + 2))" "$stock_osm2pgsql_path" stock "$georgia_pbf" "$simplified_daily_diff"
fi

if variant_enabled patched_clustered_import; then
    run_variant patched_clustered_import "$PATCHED_PG_BIN" "$((PG_PORT_BASE + 3))" "$CLUSTERED_OSM2PGSQL" clustered_import "$georgia_pbf" "$simplified_daily_diff"
fi

log "done; logs are in $WORKDIR/logs"
