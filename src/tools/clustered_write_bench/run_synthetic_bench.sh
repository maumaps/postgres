#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

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
HOT_TILE_COUNT_VALUES=${HOT_TILE_COUNT_VALUES:-"1"}
HOT_UPDATE_FRACTION_VALUES=${HOT_UPDATE_FRACTION_VALUES:-"0"}
UPDATE_PAYLOAD_REPEAT_VALUES=${UPDATE_PAYLOAD_REPEAT_VALUES:-"64"}
HEAP_FILLFACTOR_VALUES=${HEAP_FILLFACTOR_VALUES:-"90"}
ORDER_DIFF_BY_CLUSTER_KEY_VALUES=${ORDER_DIFF_BY_CLUSTER_KEY_VALUES:-"false"}
COPY_DIFF_FROM_FILE_VALUES=${COPY_DIFF_FROM_FILE_VALUES:-"false"}
UPDATES_BEFORE_INSERTS_VALUES=${UPDATES_BEFORE_INSERTS_VALUES:-"false"}
OUTDIR=${OUTDIR:-"$HOME/tmp/clustered-write-synthetic/$(date +%Y%m%d-%H%M%S)"}
COMPRESS_RAW=${COMPRESS_RAW:-true}
KEEP_TEMP_INSTANCE_DATA=${KEEP_TEMP_INSTANCE_DATA:-false}

mkdir -p "$OUTDIR/raw"

{
	printf 'date: '
	date -Is
	printf 'script_dir: %s\n' "$SCRIPT_DIR"
	printf 'repo_root: %s\n' "$REPO_ROOT"
	if git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
		printf 'git_head: %s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD)"
		printf 'git_branch: %s\n' "$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"
	fi
	printf 'dbname: %s\n' "$DBNAME"
	printf 'pg_bindir: %s\n' "$PG_BINDIR"
	printf 'use_temp_instance: %s\n' "$USE_TEMP_INSTANCE"
	printf 'pgport: %s\n' "$PGPORT"
	printf 'pg_opts: %s\n' "$PG_OPTS"
	printf 'repeats: %s\n' "$REPEATS"
	printf 'scale_values: %s\n' "$SCALE_VALUES"
	printf 'brin_values: %s\n' "$BRIN_VALUES"
	printf 'single_key_values: %s\n' "$SINGLE_KEY_VALUES"
	printf 'text_key_values: %s\n' "$TEXT_KEY_VALUES"
	printf 'hot_tile_fraction_values: %s\n' "$HOT_TILE_FRACTION_VALUES"
	printf 'hot_tile_count_values: %s\n' "$HOT_TILE_COUNT_VALUES"
	printf 'hot_update_fraction_values: %s\n' "$HOT_UPDATE_FRACTION_VALUES"
	printf 'update_payload_repeat_values: %s\n' "$UPDATE_PAYLOAD_REPEAT_VALUES"
	printf 'heap_fillfactor_values: %s\n' "$HEAP_FILLFACTOR_VALUES"
	printf 'order_diff_by_cluster_key_values: %s\n' "$ORDER_DIFF_BY_CLUSTER_KEY_VALUES"
	printf 'copy_diff_from_file_values: %s\n' "$COPY_DIFF_FROM_FILE_VALUES"
	printf 'updates_before_inserts_values: %s\n' "$UPDATES_BEFORE_INSERTS_VALUES"
	printf 'compress_raw: %s\n' "$COMPRESS_RAW"
	printf 'keep_temp_instance_data: %s\n' "$KEEP_TEMP_INSTANCE_DATA"
	printf 'uname: '
	uname -a
	printf 'uptime: '
	uptime
	df -h "$OUTDIR" "${TMPDIR:-/tmp}" 2>/dev/null || true
} >"$OUTDIR/run_environment.txt"

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

printf 'run\tscale\tbrin_enabled\tsingle_key_cluster\ttext_cluster_key\theap_fillfactor\thot_tile_fraction\thot_tile_count\thot_update_fraction\tupdate_payload_repeat\tupdates_before_inserts\torder_diff_by_cluster_key\tcopy_diff_from_file\tstep\telapsed_ms\n' >"$timings_tsv"
printf 'run\tscale\tbrin_enabled\tsingle_key_cluster\ttext_cluster_key\theap_fillfactor\thot_tile_fraction\thot_tile_count\thot_update_fraction\tupdate_payload_repeat\tupdates_before_inserts\torder_diff_by_cluster_key\tcopy_diff_from_file\tvariant\tdiff_kind\trows_measured\theap_blocks_touched\theap_block_span\toutside_base_heap_block_span\tpct_inside_base_range\tavg_block_drift\tp95_block_drift\tmax_block_drift\n' >"$locality_tsv"

for scale in $SCALE_VALUES; do
	for brin in $BRIN_VALUES; do
		for single_key in $SINGLE_KEY_VALUES; do
			for text_cluster_key in $TEXT_KEY_VALUES; do
				for heap_fillfactor in $HEAP_FILLFACTOR_VALUES; do
					for hot_tile_fraction in $HOT_TILE_FRACTION_VALUES; do
						for hot_tile_count in $HOT_TILE_COUNT_VALUES; do
							for hot_update_fraction in $HOT_UPDATE_FRACTION_VALUES; do
								for update_payload_repeat in $UPDATE_PAYLOAD_REPEAT_VALUES; do
									for updates_before_inserts in $UPDATES_BEFORE_INSERTS_VALUES; do
										for order_diff_by_cluster_key in $ORDER_DIFF_BY_CLUSTER_KEY_VALUES; do
											for copy_diff_from_file in $COPY_DIFF_FROM_FILE_VALUES; do
												for run in $(seq 1 "$REPEATS"); do
													raw="$OUTDIR/raw/scale-${scale}_brin-${brin}_single-key-${single_key}_text-key-${text_cluster_key}_fillfactor-${heap_fillfactor}_hot-tile-${hot_tile_fraction}_hot-tile-count-${hot_tile_count}_hot-update-${hot_update_fraction}_update-payload-${update_payload_repeat}_updates-before-${updates_before_inserts}_order-diff-${order_diff_by_cluster_key}_copy-${copy_diff_from_file}_run-${run}.out"
													diff_copy_path="${raw%.out}.copy.tsv"

													if ! "$PSQL" -X -v ON_ERROR_STOP=1 \
														-v scale="$scale" \
														-v use_brin="$brin" \
														-v single_key_cluster="$single_key" \
														-v text_cluster_key="$text_cluster_key" \
														-v heap_fillfactor="$heap_fillfactor" \
														-v hot_tile_fraction="$hot_tile_fraction" \
														-v hot_tile_count="$hot_tile_count" \
														-v hot_update_fraction="$hot_update_fraction" \
														-v update_payload_repeat="$update_payload_repeat" \
														-v updates_before_inserts="$updates_before_inserts" \
														-v order_diff_by_cluster_key="$order_diff_by_cluster_key" \
														-v copy_diff_from_file="$copy_diff_from_file" \
														-v diff_copy_path="$diff_copy_path" \
														-d "$DBNAME" >"$raw" <<SQL
\\pset format unaligned
\\pset border 0
\\pset footer off
\\pset pager off
\\i $SCRIPT_DIR/osm2pgsql_diff.sql
SQL
													then
														rm -f "$diff_copy_path"
														exit 1
													fi
													rm -f "$diff_copy_path"

													awk -F'|' \
														-v run="$run" \
														-v scale="$scale" \
														-v brin="$brin" \
														-v single_key="$single_key" \
														-v text_cluster_key="$text_cluster_key" \
														-v heap_fillfactor="$heap_fillfactor" \
														-v hot_tile_fraction="$hot_tile_fraction" \
														-v hot_tile_count="$hot_tile_count" \
														-v hot_update_fraction="$hot_update_fraction" \
														-v update_payload_repeat="$update_payload_repeat" \
														-v updates_before_inserts="$updates_before_inserts" \
														-v order_diff_by_cluster_key="$order_diff_by_cluster_key" \
														-v copy_diff_from_file="$copy_diff_from_file" \
														'$5 == "clustered_write_insert" ||
														 $5 == "clustered_write_update" ||
														 $5 == "clustered_write_read_hot" ||
														 $5 == "clustered_write_read_updated_hot" ||
														 $5 == "without_cluster_metadata_insert" ||
														 $5 == "without_cluster_metadata_update" ||
														 $5 == "without_cluster_metadata_read_hot" ||
														 $5 == "without_cluster_metadata_read_updated_hot" {
															printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
																run, scale, brin, single_key, text_cluster_key, heap_fillfactor, hot_tile_fraction, hot_tile_count, hot_update_fraction, update_payload_repeat, updates_before_inserts, order_diff_by_cluster_key, copy_diff_from_file, $5, $6
														}' "$raw" >>"$timings_tsv"

													awk -F'|' \
														-v run="$run" \
														-v scale="$scale" \
														-v brin="$brin" \
														-v single_key="$single_key" \
														-v text_cluster_key="$text_cluster_key" \
														-v heap_fillfactor="$heap_fillfactor" \
														-v hot_tile_fraction="$hot_tile_fraction" \
														-v hot_tile_count="$hot_tile_count" \
														-v hot_update_fraction="$hot_update_fraction" \
														-v update_payload_repeat="$update_payload_repeat" \
														-v updates_before_inserts="$updates_before_inserts" \
														-v order_diff_by_cluster_key="$order_diff_by_cluster_key" \
														-v copy_diff_from_file="$copy_diff_from_file" \
														'$5 == "clustered_write" ||
														 $5 == "without_cluster_metadata" {
															printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
																run, scale, brin, single_key, text_cluster_key, heap_fillfactor, hot_tile_fraction, hot_tile_count, hot_update_fraction, update_payload_repeat, updates_before_inserts, order_diff_by_cluster_key, copy_diff_from_file, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
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
		      "hot_tile_count", "hot_update_fraction",
		      "update_payload_repeat",
		      "updates_before_inserts",
		      "order_diff_by_cluster_key",
		      "copy_diff_from_file",
		      "step", "runs",
		      "avg_elapsed_ms", "median_elapsed_ms", "min_elapsed_ms",
		      "max_elapsed_ms"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $8 OFS $9 OFS $10 OFS $11 OFS $12 OFS $13 OFS $14
		sum[key] += $15
		count[key]++
		sample[key, count[key]] = $15
		if (!(key in min) || $15 < min[key])
			min[key] = $15
		if (!(key in max) || $15 > max[key])
			max[key] = $15
	}
	END {
		for (key in count) {
			for (i = 1; i <= count[key]; i++)
				value[i] = sample[key, i] + 0
			for (i = 1; i <= count[key]; i++) {
				for (j = i + 1; j <= count[key]; j++) {
					if (value[i] <= value[j])
						continue
					tmp = value[i]
					value[i] = value[j]
					value[j] = tmp
				}
			}
			median_pos = int((count[key] + 1) / 2)
			if (count[key] % 2)
				median[key] = value[median_pos]
			else
				median[key] = (value[median_pos] + value[median_pos + 1]) / 2
		}
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n",
				key, count[key], sum[key] / count[key], median[key],
				min[key], max[key]
	}
' "$timings_tsv" >"$timing_summary_tsv"
{
	head -n 1 "$timing_summary_tsv"
	tail -n +2 "$timing_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4 -k5,5n -k6,6V -k7,7n -k8,8 -k9,9 -k10,10n -k11,11 -k12,12 -k13,13
} >"$timing_summary_tsv.tmp"
mv "$timing_summary_tsv.tmp" "$timing_summary_tsv"

awk -F'\t' '
	BEGIN {
		OFS = "\t"
		print "scale", "brin_enabled", "single_key_cluster",
		      "text_cluster_key", "heap_fillfactor", "hot_tile_fraction",
		      "hot_tile_count", "hot_update_fraction",
		      "update_payload_repeat",
		      "updates_before_inserts",
		      "order_diff_by_cluster_key",
		      "copy_diff_from_file",
		      "variant", "diff_kind", "runs", "avg_rows_measured",
		      "avg_heap_block_span", "avg_outside_base_heap_block_span",
		      "avg_pct_inside_base_range",
		      "avg_block_drift", "avg_p95_block_drift"
	}
	NR > 1 {
		key = $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $8 OFS $9 OFS $10 OFS $11 OFS $12 OFS $13 OFS $14 OFS $15
		rows[key] += $16
		span[key] += $18
		outside_span[key] += $19
		pct[key] += $20
		avg[key] += $21
		p95[key] += $22
		count[key]++
	}
	END {
		for (key in count)
			printf "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",
				key, count[key], rows[key] / count[key], span[key] / count[key],
				outside_span[key] / count[key], pct[key] / count[key],
				avg[key] / count[key], p95[key] / count[key]
	}
' "$locality_tsv" >"$locality_summary_tsv"
{
	head -n 1 "$locality_summary_tsv"
	tail -n +2 "$locality_summary_tsv" | sort -t '	' -k1,1V -k2,2 -k3,3 -k4,4 -k5,5n -k6,6V -k7,7n -k8,8 -k9,9 -k10,10n -k11,11 -k12,12 -k13,13 -k14,14
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
