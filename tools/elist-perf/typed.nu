#!/usr/bin/env nu
use compare.nu run-saved

def main [source: path, output: path, --benchtime: string = "300ms", --profiletime: string = "2s"] {
    let source = ($source | path expand)
    let output = ($output | path expand)
    if $source == $output { error make {msg: "Output must differ from source"} }
    mkdir $output
    cd $source
    let order = [Readme Typed RuntimeOffset Local Local RuntimeOffset Typed Readme Readme RuntimeOffset Local Typed]
    with-env {GOTOOLCHAIN: go1.27.1} {
        {
            revision: (git rev-parse HEAD | str trim)
            status: (git status --porcelain=v1)
            go: (go version | str trim)
            env: (go env GOOS GOARCH GOAMD64)
            order: $order
            benchtime: $benchtime
            profiletime: $profiletime
            cpu: 1
        } | save -f ($output | path join manifest.nuon)
        let binary = ($output | path join list.test)
        run-saved go ['test' '-c' '-o' $binary] ($output | path join build)
        for turn in ($order | enumerate) {
            run-saved $binary [
                '-test.run' '^$' '-test.bench' ('^BenchmarkList$/^(16|1024)$/^' + $turn.item + '$/')
                '-test.benchtime' $benchtime '-test.cpu' '1' '-test.benchmem'
            ] ($output | path join $"timing-($turn.index)-($turn.item)")
        }
        run-saved $binary [
            '-test.run' '^$' '-test.bench' '^BenchmarkListView$'
            '-test.benchtime' $benchtime '-test.cpu' '1' '-test.benchmem'
        ] ($output | path join sizes)
        for mode in [Readme Typed RuntimeOffset Local] {
            let operations = [Walk DirectWalk]
            for operation in $operations {
                let stem = ($output | path join $"($mode)-($operation)")
                run-saved $binary [
                    '-test.run' '^$' '-test.bench' $"^BenchmarkList$/^1024$/^($mode)$/^($operation)$"
                    '-test.benchtime' $profiletime '-test.cpu' '1'
                    '-test.cpuprofile' $"($stem).cpu.pprof"
                ] $"($stem)-profile"
                run-saved go ['tool' 'pprof' '-top' '-nodecount=12' $binary $"($stem).cpu.pprof"] $"($stem)-top"
            }
        }
    }
    let rows = (glob ($output | path join 'timing-*.txt')
        | where {|p| not ($p | path basename | str ends-with '.stderr.txt')}
        | each {|p| open --raw $p | lines
            | parse -r '^BenchmarkList/(?<size>\d+)/(?<variant>[^/]+)/(?<operation>\S+)\s+\d+\s+(?<ns>[0-9.]+) ns/op\s+(?<bytes>\d+) B/op\s+(?<allocs>\d+) allocs/op'
        } | flatten)
    if ($rows | is-empty) { error make {msg: "No typed benchmark measurements"} }
    let summary = ($rows | group-by size variant operation --to-table | each {|g|
        if ($g.items | length) != 3 { error make {msg: "Expected three measurements per operation"} }
        {
            size: $g.size, variant: $g.variant, operation: $g.operation
            ns: ($g.items.ns | into float | math median)
            bytes: ($g.items.bytes | into int | math median)
            allocs: ($g.items.allocs | into int | math median)
        }
    })
    if ($summary | length) != 24 or (($summary.size | uniq | sort) != ['1024' '16']) {
        error make {msg: "Expected all 24 size/variant/operation cases for 16 and 1024 elements"}
    }
    $summary | save -f ($output | path join summary.nuon)
    print $"Typed comparison saved to ($output)"
}
