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
OUTDIR=${OUTDIR:-"$HOME/tmp/clustered-write-synthetic/$(date +%Y%m%d-%H%M%S)"}

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

printf 'run\tscale\tbrin_enabled\tstep\telapsed_ms\n' >"$timings_tsv"
printf 'run\tscale\tbrin_enabled\tvariant\tdiff_kind\trows_measured\theap_blocks_touched\tpct_inside_base_range\tavg_block_drift\tp95_block_drift\tmax_block_drift\n' >"$locality_tsv"

for scale in $SCALE_VALUES; do
	for brin in $BRIN_VALUES; do
		for run in $(seq 1 "$REPEATS"); do
			raw="$OUTDIR/raw/scale-${scale}_brin-${brin}_run-${run}.out"

			"$PSQL" -X -v ON_ERROR_STOP=1 \
				-v scale="$scale" \
				-v use_brin="$brin" \
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
				'$2 == "clustered_write_insert" ||
				 $2 == "clustered_write_update" ||
				 $2 == "without_cluster_metadata_insert" ||
				 $2 == "without_cluster_metadata_update" {
					printf "%s\t%s\t%s\t%s\t%s\n", run, scale, brin, $2, $3
				}' "$raw" >>"$timings_tsv"

			awk -F'|' \
				-v run="$run" \
				-v scale="$scale" \
				-v brin="$brin" \
				'$2 == "clustered_write" ||
				 $2 == "without_cluster_metadata" {
					printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
						run, scale, brin, $2, $3, $4, $5, $6, $7, $8, $9
				}' "$raw" >>"$locality_tsv"
		done
	done
done

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "step", "runs", "avg_elapsed_ms", "min_elapsed_ms", "max_elapsed_ms"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4
		sum[key] += $5
		count[key]++
		if (!(key in min) || $5 < min[key])
			min[key] = $5
		if (!(key in max) || $5 > max[key])
			max[key] = $5
	}
	END {
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\n",
				key, count[key], sum[key] / count[key], min[key], max[key]
	}
' "$timings_tsv" >"$timing_summary_tsv"
{
	head -n 1 "$timing_summary_tsv"
	tail -n +2 "$timing_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3
} >"$timing_summary_tsv.tmp"
mv "$timing_summary_tsv.tmp" "$timing_summary_tsv"

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "variant", "diff_kind", "runs",
		      "avg_pct_inside_base_range", "avg_block_drift", "avg_p95_block_drift"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5
		pct[key] += $8
		avg[key] += $9
		p95[key] += $10
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
	tail -n +2 "$locality_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4
} >"$locality_summary_tsv.tmp"
mv "$locality_summary_tsv.tmp" "$locality_summary_tsv"

printf 'raw output: %s/raw\n' "$OUTDIR"
printf 'timings: %s\n' "$timings_tsv"
printf 'locality: %s\n' "$locality_tsv"
printf 'timing summary: %s\n' "$timing_summary_tsv"
printf 'locality summary: %s\n' "$locality_summary_tsv"
