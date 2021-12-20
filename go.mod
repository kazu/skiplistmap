module github.com/kazu/skiplistmap

go 1.17

require (
	github.com/cespare/xxhash v1.1.0
	github.com/cornelk/hashmap v1.0.1
	github.com/kazu/elist_head v0.2.6
	github.com/kazu/loncha v0.4.9
	github.com/lrita/cmap v0.0.0-20200818170753-e987cd3dfa73
	github.com/stretchr/testify v1.7.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dchest/siphash v1.1.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/seehuhn/mt19937 v0.0.0-20180715112136-cc7708819361 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

// replace github.com/kazu/loncha => ../loncha/
// replace github.com/kazu/elist_head => ../elist_head/
