#!/usr/bin/env nu

# Run from any directory. Sources and output must be distinct directories.
def main [
    original: path
    modified: path
    output: path
    --benchtime: string = "300ms"
    --profiletime: string = "3s"
] {
    let original = ($original | path expand)
    let modified = ($modified | path expand)
    let output = ($output | path expand)
    if $original == $modified or $output == $original or $output == $modified {
        error make {msg: "Source directories and output must be distinct"}
    }
    mkdir $output
    let baseline = "eefadded7d74d2d4badce57bbf9541264d386ded"
    let production_diff = (do {
        cd $original
        git diff --name-only $baseline -- . ':(exclude)*_test.go' | complete
    })
    if $production_diff.exit_code != 0 or ($production_diff.stdout | str trim) != "" {
        error make {msg: "Original production source differs from eefadded"}
    }
    let cases = [
        {name: next-normal, pattern: '^Benchmark_Next$/^list_head$'}
        {name: next-relative, pattern: '^Benchmark_Next$/^elist_head$'}
        {name: walk16, pattern: '^BenchmarkMigration$/^16$/^Walk$'}
        {name: walk1024, pattern: '^BenchmarkMigration$/^1024$/^Walk$'}
        {name: payload1024, pattern: '^BenchmarkMigration$/^1024$/^PayloadWalk$'}
        {name: repair1024, pattern: '^BenchmarkCopyRepair$/^1024$'}
        {name: copy1024, pattern: '^BenchmarkCopyOnly$/^1024$'}
    ]
    with-env {GOTOOLCHAIN: go1.27.1} {
        let sources = [{name: original, path: $original}, {name: modified, path: $modified}]
        let revisions = ($sources | each {|source|
            cd $source.path
            let revision = (git rev-parse HEAD | str trim)
            let status = (git status --porcelain=v1)
            {name: $source.name, path: $source.path, revision: $revision, status: $status}
        })
        {
            sources: $revisions
            baseline: $baseline
            go: (go version | str trim)
            env: (go env GOOS GOARCH GOAMD64)
            uname: (^uname -a | str trim)
            timing_order: [original modified modified original original modified]
            benchtime: $benchtime
            profiletime: $profiletime
            cpu: 1
            cases: $cases
        } | save -f ($output | path join manifest.nuon)
        for source in $sources {
            cd $source.path
            let binary = ($output | path join $"($source.name).test")
            run-saved go ['test' '-c' '-o' $binary] ($output | path join $"($source.name)-build")
        }
        let timing_pattern = '^Benchmark_Next$|^BenchmarkMigration$|^BenchmarkCopyRepair$|^BenchmarkCopyOnly$'
        for turn in ([original modified modified original original modified] | enumerate) {
            let binary = ($output | path join $"($turn.item).test")
            run-saved $binary [
                '-test.run' '^$' '-test.bench' $timing_pattern
                '-test.benchtime' $benchtime '-test.cpu' '1' '-test.benchmem'
            ] ($output | path join $"timing-($turn.index)-($turn.item)")
        }
        for case in $cases {
            for source in $sources {
                let binary = ($output | path join $"($source.name).test")
                let stem = ($output | path join $"($case.name)-($source.name)")
                run-saved $binary [
                    '-test.run' '^$' '-test.bench' $case.pattern
                    '-test.benchtime' $profiletime '-test.cpu' '1'
                    '-test.cpuprofile' $"($stem).cpu.pprof"
                    '-test.memprofile' $"($stem).mem.pprof"
                ] $"($stem)-profile"
                for kind in [cpu mem] {
                    run-saved go [
                        'tool' 'pprof' '-top' '-nodecount=15' $binary $"($stem).($kind).pprof"
                    ] $"($stem)-($kind)-top"
                }
            }
        }
        print $"Comparison saved to ($output)"
    }
}

export def run-saved [command: string, args: list<string>, stem: string] {
    let result = (^$command ...$args | complete)
    $result.stdout | save -f $"($stem).txt"
    $result.stderr | save -f $"($stem).stderr.txt"
    if $result.exit_code != 0 {
        error make {msg: $"($command) failed: ($result.stderr); see ($stem).txt"}
    }
    if ('-test.bench' in $args) and not ($result.stdout | lines | any {|line| $line | str starts-with 'Benchmark'}) {
        error make {msg: $"No benchmark matched; see ($stem).txt"}
    }
}
