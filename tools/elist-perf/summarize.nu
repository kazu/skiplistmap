#!/usr/bin/env nu

def main [output: path] {
    let output = ($output | path expand)
    let rows = (glob ($output | path join 'timing-*.txt')
        | where {|p| not ($p | path basename | str ends-with '.stderr.txt')}
        | each {|file|
            let variant = ($file | path basename
                | parse -r '^timing-\d+-(?<variant>original|modified)\.txt$' | get 0.variant)
            open --raw $file | lines
                | parse -r '^(?<benchmark>Benchmark\S+)\s+(?<iterations>\d+)\s+(?<ns>[0-9.]+) ns/op\s+(?<bytes>\d+) B/op\s+(?<allocs>\d+) allocs/op'
                | insert variant $variant
        } | flatten)
    if ($rows | is-empty) {
        error make {msg: "No benchmark measurements found"}
    }
    let summary = ($rows | group-by benchmark --to-table | each {|group|
        let original = ($group.items | where variant == original)
        let modified = ($group.items | where variant == modified)
        if ($original | length) != 3 or ($modified | length) != 3 {
            error make {msg: $"Expected three measurements per variant: ($group.benchmark)"}
        }
        let old = ($original.ns | into float | math median)
        let new = ($modified.ns | into float | math median)
        {
            benchmark: $group.benchmark
            original_ns: $old
            modified_ns: $new
            ratio: ($new / $old)
            original_bytes: ($original.bytes | into int | math median)
            modified_bytes: ($modified.bytes | into int | math median)
            original_allocs: ($original.allocs | into int | math median)
            modified_allocs: ($modified.allocs | into int | math median)
        }
    })
    $rows | save -f ($output | path join timing.nuon)
    $summary | save -f ($output | path join summary.nuon)
    $summary
}
