#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
ulimit -v 4194304
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.27.1}"
export GOMAXPROCS="${GOMAXPROCS:-4}"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/skiplistmap-ci.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
root=$PWD
elist_root=$(go list -m -f '{{.Dir}}' github.com/kazu/elist_head)
loncha_root=$(go list -m -f '{{.Dir}}' github.com/kazu/loncha)
modes=("$@")
if ((${#modes[@]} == 0)); then
	modes=(normal race checkptr step)
fi

go version
for mode in "${modes[@]}"; do
	case "$mode" in
		normal) flags=() ;;
		race) flags=(-race) ;;
		checkptr) flags=(-gcflags=all=-d=checkptr) ;;
		step) flags=(-race -tags=stephook) ;;
		*) echo "Unknown mode: $mode" >&2; exit 2 ;;
	esac
	for module in "$root" "$elist_root" "$loncha_root"; do
		cd "$module"
		pattern=./...
		if [[ "$module" == "$loncha_root" ]]; then
			# This is the loncha package imported by skiplistmap.
			pattern=./lista_encabezado
		fi
		if [[ "$mode" == normal ]]; then
			go vet "$pattern"
		fi
		go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' "$pattern" > "$scratch/packages"
		while IFS= read -r package; do
			[[ -n "$package" ]] || continue
			if [[ "$mode" == step ]]; then
				case "$package" in
					github.com/kazu/skiplistmap/rmap) tests='^(Test|Example|Fuzz)' ;;
					github.com/kazu/skiplistmap) tests='^Test_(J(51|56)|OperationsDistinguishSameHashPair|EmbeddedGetDoesNotReadReusedSlot|EmbeddedRangeKeepsKeyAndValueTogether|DifferentKeysWithSameHashPair)|^Test(Typed|EntryCopy|_PurgeAndSet|_StepRaceSplit|_ConcurrentUpdateWhileGrowing)' ;;
					github.com/kazu/elist_head) tests='^TestReplace(Node|WithAdjacent)' ;;
					github.com/kazu/loncha/lista_encabezado) tests='^TestLenRestartsAfterCurrentNodeIsDeleted$' ;;
					*) continue ;;
				esac
			else
				tests='^(Test|Example|Fuzz)'
			fi
			go test -c "${flags[@]}" -o "$scratch/tests" "$package"
			"$scratch/tests" -test.list "$tests" > "$scratch/names"
			while IFS= read -r name; do
				[[ "$name" =~ ^(Test|Example|Fuzz)[[:alnum:]_]*$ ]] || continue
				cases=("$name")
				if [[ "$package" == github.com/kazu/skiplistmap && "$mode" != normal && "$mode" != checkptr ]]; then
					case "$name" in
						Test_ConcurrentDeleteReinsert|Test_ConcurrentUpdateWhileGrowing)
							# All four crashMapParams, with the full workload in each.
							# The race allocator retains metadata across subtests too.
							cases=()
							for variant in 5 4; do
								for buckets in 16 32; do
									cases+=("$name/skiplistmap${variant}_bucket=$buckets")
								done
							done ;;
					esac
				fi
				for test_case in "${cases[@]}"; do
					# Isolate tests so race-detector allocations do not accumulate
					# across the entire suite under the 4 GiB virtual-memory limit.
					if /usr/bin/time -v "$scratch/tests" -test.run "^${test_case/\//\$\/\^}\$" -test.count=1 -test.timeout=300s > "$scratch/result" 2>&1; then
						printf 'PASS %s %s %s\n' "$mode" "$package" "$test_case"
					else
						status=$?
						printf 'FAIL %s %s %s (exit %s)\n' "$mode" "$package" "$test_case" "$status"
						cat "$scratch/result"
						exit "$status"
					fi
				done
			done < "$scratch/names"
		done < "$scratch/packages"
	done
done
