#!/usr/bin/env nu

def main [] {
    let output = (^mktemp -d -t elist-original-stderr-XXXXXX | str trim)
    try {
        for turn in ([original modified modified original original modified] | enumerate) {
            let ns = if $turn.item == original { 10 } else { 30 }
            $"BenchmarkExample 100 ($ns) ns/op 0 B/op 0 allocs/op(char nl)"
                | save ($output | path join $"timing-($turn.index)-($turn.item).txt")
        }
        let script = ($env.FILE_PWD | path join summarize.nu)
        let result = (nu $script $output | complete)
        if $result.exit_code != 0 {
            error make {msg: $result.stderr}
        }
        let row = (open ($output | path join summary.nuon) | first)
        if $row.original_ns != 10 or $row.modified_ns != 30 or $row.ratio != 3 {
            error make {msg: "Output directory name changed variant classification"}
        }
    } catch {|error|
        rm -r $output
        error make {msg: $error.msg}
    }
    rm -r $output
    print "summary path regression: PASS"
}
