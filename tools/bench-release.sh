#!/usr/bin/env bash
set -euo pipefail

ulimit -v "${BENCH_MEMORY_KB:-8388608}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.27.1}"
export GOMAXPROCS="${GOMAXPROCS:-16}"
result_dir=${1:?Usage: bash tools/bench-release.sh RESULT_DIR}
benchtime=${BENCHTIME:-500ms}
repeats=${BENCH_COUNT:-5}
mkdir -p "$result_dir"
scratch=$(mktemp -d "$result_dir/bench.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

{
	git rev-parse HEAD
	git status --porcelain
	go version
	go env GOOS GOARCH GOAMD64 CGO_ENABLED
	printf 'GOGC=%s GOMEMLIMIT=%s GOFLAGS=%s\n' "${GOGC:-default}" "${GOMEMLIMIT:-default}" "${GOFLAGS:-}"
	go list -m all
	for module in github.com/kazu/elist_head github.com/kazu/lista_encabezado; do
		module_dir=$(go list -m -f '{{.Dir}}' "$module")
		if [[ -e "$module_dir/.git" ]]; then
			printf '%s checkout: ' "$module"
			git -C "$module_dir" rev-parse HEAD
			git -C "$module_dir" status --porcelain
		fi
	done
	printf 'GOMAXPROCS=%s requested_workers=64 actual_workers=%s records=100000 benchtime=%s repeats=%s memory_limit_KB=%s\n' \
		"$GOMAXPROCS" "$(((64 + GOMAXPROCS - 1) / GOMAXPROCS * GOMAXPROCS))" \
		"$benchtime" "$repeats" "$(ulimit -v)"
	lscpu
} > "$result_dir/environment.txt"
go test -c -o "$scratch/bench" .
/usr/bin/time -v "$scratch/bench" -test.run '^$' -test.bench '^Benchmark_Map$' \
	-test.benchtime=1x -test.count=1 -test.timeout=5m > "$result_dir/discovery.log" 2>&1
awk '$1 ~ /^Benchmark_Map\// {sub(/-[0-9]+$/, "", $1); print $1}' \
	"$result_dir/discovery.log" > "$result_dir/cases.txt"
mapfile -t cases < "$result_dir/cases.txt"
((${#cases[@]} > 0))
: > "$result_dir/results.txt"
printf 'round\tcase\tmax_rss_KB\telapsed\n' > "$result_dir/resources.tsv"
for phase in readonly mixed; do
for ((round=1; round<=repeats; round++)); do
	for ((position=0; position<${#cases[@]}; position++)); do
		index=$position
		if ((round % 2 == 0)); then
			index=$((${#cases[@]} - position - 1))
		fi
		case_name=${cases[index]}
        if [[ $phase == readonly && $case_name != *w/__0_u/* ]]; then continue; fi
        if [[ $phase == mixed && $case_name == *w/__0_u/* ]]; then continue; fi
		pattern=${case_name//./\\.}
		pattern="^${pattern//\//$\/^}\$"
		printf 'round %s/%s case %s/%s %s\n' "$round" "$repeats" \
			"$((position + 1))" "${#cases[@]}" "$case_name"
		if ! /usr/bin/time -v "$scratch/bench" -test.run '^$' -test.bench "$pattern" \
			-test.benchtime="$benchtime" -test.count=1 -test.timeout=5m > "$scratch/result" 2>&1; then
			cp "$scratch/result" "$result_dir/failure.log"
			cat "$scratch/result"
			exit 1
		fi
		awk '$1 ~ /^Benchmark_Map\// {print; found=1} END {if (!found) exit 1}' \
			"$scratch/result" >> "$result_dir/results.txt"
		awk -v round="$round" -v name="$case_name" '
			/Maximum resident set size/ {rss=$NF}
			/Elapsed \(wall clock\)/ {elapsed=$NF}
			END {printf "%s\t%s\t%s\t%s\n", round, name, rss, elapsed}
		' "$scratch/result" >> "$result_dir/resources.tsv"
	done
done
printf "phase %s complete\n" "$phase"
done
