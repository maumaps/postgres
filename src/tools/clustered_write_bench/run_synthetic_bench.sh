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
HEAP_FILLFACTOR_VALUES=${HEAP_FILLFACTOR_VALUES:-"90"}
ORDER_DIFF_BY_CLUSTER_KEY_VALUES=${ORDER_DIFF_BY_CLUSTER_KEY_VALUES:-"false"}
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
	PGHOST=$(mktemp -d "${TMPDIR:-/tmp}/clustered-write-bench-socket.XXXXXX")
	export PGHOST PGPORT
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
			rm -rf "$PGDATA"
		fi
		rm -rf "$PGHOST"
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

printf 'run\tscale\tbrin_enabled\tsingle_key_cluster\ttext_cluster_key\theap_fillfactor\thot_tile_fraction\torder_diff_by_cluster_key\tstep\telapsed_ms\n' >"$timings_tsv"
printf 'run\tscale\tbrin_enabled\tsingle_key_cluster\ttext_cluster_key\theap_fillfactor\thot_tile_fraction\torder_diff_by_cluster_key\tvariant\tdiff_kind\trows_measured\theap_blocks_touched\theap_block_span\tpct_inside_base_range\tavg_block_drift\tp95_block_drift\tmax_block_drift\n' >"$locality_tsv"

for scale in $SCALE_VALUES; do
	for brin in $BRIN_VALUES; do
		for single_key in $SINGLE_KEY_VALUES; do
			for text_cluster_key in $TEXT_KEY_VALUES; do
				for heap_fillfactor in $HEAP_FILLFACTOR_VALUES; do
					for hot_tile_fraction in $HOT_TILE_FRACTION_VALUES; do
						for order_diff_by_cluster_key in $ORDER_DIFF_BY_CLUSTER_KEY_VALUES; do
							for run in $(seq 1 "$REPEATS"); do
								raw="$OUTDIR/raw/scale-${scale}_brin-${brin}_single-key-${single_key}_text-key-${text_cluster_key}_fillfactor-${heap_fillfactor}_hot-tile-${hot_tile_fraction}_order-diff-${order_diff_by_cluster_key}_run-${run}.out"

								"$PSQL" -X -v ON_ERROR_STOP=1 \
									-v scale="$scale" \
									-v use_brin="$brin" \
									-v single_key_cluster="$single_key" \
									-v text_cluster_key="$text_cluster_key" \
									-v heap_fillfactor="$heap_fillfactor" \
									-v hot_tile_fraction="$hot_tile_fraction" \
									-v order_diff_by_cluster_key="$order_diff_by_cluster_key" \
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
									-v heap_fillfactor="$heap_fillfactor" \
									-v hot_tile_fraction="$hot_tile_fraction" \
									-v order_diff_by_cluster_key="$order_diff_by_cluster_key" \
									'$4 == "clustered_write_insert" ||
									 $4 == "clustered_write_update" ||
									 $4 == "without_cluster_metadata_insert" ||
									 $4 == "without_cluster_metadata_update" {
										printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
											run, scale, brin, single_key, text_cluster_key, heap_fillfactor, hot_tile_fraction, order_diff_by_cluster_key, $4, $5
									}' "$raw" >>"$timings_tsv"

								awk -F'|' \
									-v run="$run" \
									-v scale="$scale" \
									-v brin="$brin" \
									-v single_key="$single_key" \
									-v text_cluster_key="$text_cluster_key" \
									-v heap_fillfactor="$heap_fillfactor" \
									-v hot_tile_fraction="$hot_tile_fraction" \
									-v order_diff_by_cluster_key="$order_diff_by_cluster_key" \
									'$4 == "clustered_write" ||
									 $4 == "without_cluster_metadata" {
										printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
											run, scale, brin, single_key, text_cluster_key, heap_fillfactor, hot_tile_fraction, order_diff_by_cluster_key, $4, $5, $6, $7, $8, $9, $10, $11, $12
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
	done
done

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "single_key_cluster",
		      "text_cluster_key", "heap_fillfactor", "hot_tile_fraction",
		      "order_diff_by_cluster_key", "step", "runs",
		      "avg_elapsed_ms", "min_elapsed_ms", "max_elapsed_ms"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $8 OFS $9
		sum[key] += $10
		count[key]++
		if (!(key in min) || $10 < min[key])
			min[key] = $10
		if (!(key in max) || $10 > max[key])
			max[key] = $10
	}
	END {
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\n",
				key, count[key], sum[key] / count[key], min[key], max[key]
	}
' "$timings_tsv" >"$timing_summary_tsv"
{
	head -n 1 "$timing_summary_tsv"
	tail -n +2 "$timing_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4 -k5,5n -k6,6V -k7,7 -k8,8
} >"$timing_summary_tsv.tmp"
mv "$timing_summary_tsv.tmp" "$timing_summary_tsv"

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "single_key_cluster",
		      "text_cluster_key", "heap_fillfactor", "hot_tile_fraction",
		      "order_diff_by_cluster_key", "variant", "diff_kind", "runs",
		      "avg_heap_block_span", "avg_pct_inside_base_range",
		      "avg_block_drift", "avg_p95_block_drift"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $8 OFS $9 OFS $10
		span[key] += $13
		pct[key] += $14
		avg[key] += $15
		p95[key] += $16
		count[key]++
	}
	END {
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n",
				key, count[key], span[key] / count[key],
				pct[key] / count[key], avg[key] / count[key],
				p95[key] / count[key]
	}
' "$locality_tsv" >"$locality_summary_tsv"
{
	head -n 1 "$locality_summary_tsv"
	tail -n +2 "$locality_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4 -k5,5n -k6,6V -k7,7 -k8,8 -k9,9
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
