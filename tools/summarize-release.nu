#!/usr/bin/env nu

def main [results: path] {
    let rows = (open --raw $results | lines | parse -r '^(?<case>Benchmark_Map/\S+)\s+(?<iterations>\d+)\s+(?<ns>[0-9.e+\-]+) ns/op\s+(?<failed>[0-9.e+\-]+) failed-writes/op\s+(?<bytes>\d+) B/op\s+(?<allocs>\d+) allocs/op')
    $rows | group-by case --to-table | each {|group|
        let samples = $group.items
        let ns = ($samples | get ns | into float)
        {
            case: $group.case
            samples: ($samples | length)
            ns_per_op: ($ns | math median)
            min_ns: ($ns | math min)
            max_ns: ($ns | math max)
            bytes_per_op: ($samples | get bytes | into int | math median)
            allocs_per_op: ($samples | get allocs | into int | math median)
            failed_writes_per_op: ($samples | get failed | into float | math median)
        }
    } | sort-by case | to csv
}
