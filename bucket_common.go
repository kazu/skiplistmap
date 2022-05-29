package skiplistmap

const (
	bucketStateNone   uint32 = 0
	bucketStateInit   uint32 = 1
	bucketStateActive uint32 = 2
)

type commonOpt struct {
	onOk bool
}

type cOptFn func(opt *commonOpt) cOptFn

func useOnOk(t bool) cOptFn {

	return func(opt *commonOpt) cOptFn {
		prev := opt.onOk
		opt.onOk = t
		return useOnOk(prev)
	}
}
func (o *commonOpt) Option(opts ...cOptFn) (prevs []cOptFn) {

	for i := range opts {
		prevs = append(prevs, opts[i](o))
	}

	return
}
