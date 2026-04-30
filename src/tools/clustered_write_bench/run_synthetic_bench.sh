#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

DBNAME=${DBNAME:-postgres}
PSQL=${PSQL:-psql}
PG_BINDIR=${PG_BINDIR:-}
USE_TEMP_INSTANCE=${USE_TEMP_INSTANCE:-false}
PGPORT=${PGPORT:-6543}
PG_OPTS=${PG_OPTS:-}
REPEATS=${REPEATS:-3}
SCALE_VALUES=${SCALE_VALUES:-"0.1 1"}
BRIN_VALUES=${BRIN_VALUES:-"false true"}
SINGLE_KEY_VALUES=${SINGLE_KEY_VALUES:-"false"}
TEXT_KEY_VALUES=${TEXT_KEY_VALUES:-"false"}
HOT_TILE_FRACTION_VALUES=${HOT_TILE_FRACTION_VALUES:-"0"}
OUTDIR=${OUTDIR:-"$HOME/tmp/clustered-write-synthetic/$(date +%Y%m%d-%H%M%S)"}
COMPRESS_RAW=${COMPRESS_RAW:-true}
KEEP_TEMP_INSTANCE_DATA=${KEEP_TEMP_INSTANCE_DATA:-false}

mkdir -p "$OUTDIR/raw"

if [[ "$USE_TEMP_INSTANCE" == "true" ]]; then
	if [[ -z "$PG_BINDIR" ]]; then
		printf 'USE_TEMP_INSTANCE=true requires PG_BINDIR=/path/to/postgres/bin\n' >&2
		exit 1
	fi

	PGDATA="$OUTDIR/pgdata"
	PGHOST="$OUTDIR/socket"
	export PGHOST PGPORT
	mkdir -p "$PGHOST"
	PSQL="$PG_BINDIR/psql"

	"$PG_BINDIR/initdb" -D "$PGDATA" --auth trust --no-sync --no-instructions \
		>"$OUTDIR/initdb.log"
	"$PG_BINDIR/pg_ctl" -D "$PGDATA" -l "$OUTDIR/postgres.log" \
		-o "-k $PGHOST -p $PGPORT -c listen_addresses='' -c fsync=off -c synchronous_commit=off -c full_page_writes=off $PG_OPTS" \
		start >"$OUTDIR/pg_ctl_start.log"

	stop_temp_instance()
	{
		"$PG_BINDIR/pg_ctl" -D "$PGDATA" stop -m fast \
			>"$OUTDIR/pg_ctl_stop.log" 2>&1 || true
		if [[ "$KEEP_TEMP_INSTANCE_DATA" != "true" ]]; then
			rm -rf "$PGDATA" "$PGHOST"
		fi
	}
	trap stop_temp_instance EXIT
fi

"$PSQL" -X -v ON_ERROR_STOP=1 -d "$DBNAME" \
	-c 'select version() as benchmark_postgres_version' \
	>"$OUTDIR/server_version.txt"

timings_tsv="$OUTDIR/timings.tsv"
locality_tsv="$OUTDIR/locality.tsv"
timing_summary_tsv="$OUTDIR/timing_summary.tsv"
locality_summary_tsv="$OUTDIR/locality_summary.tsv"

printf 'run\tscale\tbrin_enabled\tsingle_key_cluster\ttext_cluster_key\thot_tile_fraction\tstep\telapsed_ms\n' >"$timings_tsv"
printf 'run\tscale\tbrin_enabled\tsingle_key_cluster\ttext_cluster_key\thot_tile_fraction\tvariant\tdiff_kind\trows_measured\theap_blocks_touched\tpct_inside_base_range\tavg_block_drift\tp95_block_drift\tmax_block_drift\n' >"$locality_tsv"

for scale in $SCALE_VALUES; do
	for brin in $BRIN_VALUES; do
		for single_key in $SINGLE_KEY_VALUES; do
			for text_cluster_key in $TEXT_KEY_VALUES; do
				for hot_tile_fraction in $HOT_TILE_FRACTION_VALUES; do
					for run in $(seq 1 "$REPEATS"); do
						raw="$OUTDIR/raw/scale-${scale}_brin-${brin}_single-key-${single_key}_text-key-${text_cluster_key}_hot-tile-${hot_tile_fraction}_run-${run}.out"

						"$PSQL" -X -v ON_ERROR_STOP=1 \
							-v scale="$scale" \
							-v use_brin="$brin" \
							-v single_key_cluster="$single_key" \
							-v text_cluster_key="$text_cluster_key" \
							-v hot_tile_fraction="$hot_tile_fraction" \
							-d "$DBNAME" >"$raw" <<SQL
\\pset format unaligned
\\pset border 0
\\pset footer off
\\pset pager off
\\i $SCRIPT_DIR/osm2pgsql_diff.sql
SQL

						awk -F'|' \
							-v run="$run" \
							-v scale="$scale" \
							-v brin="$brin" \
							-v single_key="$single_key" \
							-v text_cluster_key="$text_cluster_key" \
							-v hot_tile_fraction="$hot_tile_fraction" \
							'$3 == "clustered_write_insert" ||
							 $3 == "clustered_write_update" ||
							 $3 == "without_cluster_metadata_insert" ||
							 $3 == "without_cluster_metadata_update" {
								printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
									run, scale, brin, single_key, text_cluster_key, hot_tile_fraction, $3, $4
							}' "$raw" >>"$timings_tsv"

						awk -F'|' \
							-v run="$run" \
							-v scale="$scale" \
							-v brin="$brin" \
							-v single_key="$single_key" \
							-v text_cluster_key="$text_cluster_key" \
							-v hot_tile_fraction="$hot_tile_fraction" \
							'$3 == "clustered_write" ||
							 $3 == "without_cluster_metadata" {
								printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
									run, scale, brin, single_key, text_cluster_key, hot_tile_fraction, $3, $4, $5, $6, $7, $8, $9, $10
							}' "$raw" >>"$locality_tsv"

						if [[ "$COMPRESS_RAW" == "true" ]]; then
							gzip -f "$raw"
						fi
					done
				done
			done
		done
	done
done

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "single_key_cluster",
		      "text_cluster_key", "hot_tile_fraction", "step", "runs",
		      "avg_elapsed_ms", "min_elapsed_ms", "max_elapsed_ms"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7
		sum[key] += $8
		count[key]++
		if (!(key in min) || $8 < min[key])
			min[key] = $8
		if (!(key in max) || $8 > max[key])
			max[key] = $8
	}
	END {
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\n",
				key, count[key], sum[key] / count[key], min[key], max[key]
	}
' "$timings_tsv" >"$timing_summary_tsv"
{
	head -n 1 "$timing_summary_tsv"
	tail -n +2 "$timing_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4 -k5,5V -k6,6
} >"$timing_summary_tsv.tmp"
mv "$timing_summary_tsv.tmp" "$timing_summary_tsv"

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "single_key_cluster",
		      "text_cluster_key", "hot_tile_fraction", "variant",
		      "diff_kind", "runs", "avg_pct_inside_base_range",
		      "avg_block_drift", "avg_p95_block_drift"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $8
		pct[key] += $11
		avg[key] += $12
		p95[key] += $13
		count[key]++
	}
	END {
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\n",
				key, count[key], pct[key] / count[key],
				avg[key] / count[key], p95[key] / count[key]
	}
' "$locality_tsv" >"$locality_summary_tsv"
{
	head -n 1 "$locality_summary_tsv"
	tail -n +2 "$locality_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4 -k5,5V -k6,6 -k7,7
} >"$locality_summary_tsv.tmp"
mv "$locality_summary_tsv.tmp" "$locality_summary_tsv"

printf 'raw output: %s/raw\n' "$OUTDIR"
printf 'timings: %s\n' "$timings_tsv"
printf 'locality: %s\n' "$locality_tsv"
printf 'timing summary: %s\n' "$timing_summary_tsv"
printf 'locality summary: %s\n' "$locality_summary_tsv"
if [[ "$COMPRESS_RAW" == "true" ]]; then
	printf 'raw files compressed: true\n'
fi
if [[ "$USE_TEMP_INSTANCE" == "true" && "$KEEP_TEMP_INSTANCE_DATA" != "true" ]]; then
	printf 'temporary instance data removed: true\n'
fi
