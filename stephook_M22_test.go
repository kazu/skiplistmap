//go:build stephook

package skiplistmap

import "fmt"

// StepCheckBucketList walks the list of buckets of h forward from its head,
// in the direction addBucket walks it, and returns the reverses it passed.
// It stops with an error at the first break: a reverse above the one before
// it, a bucket linked to itself, or a list that does not reach its tail in
// max steps.
func StepCheckBucketList(h *Map[StringKey, any],

	max int) ([]uint64, error) {
	var got []uint64
	for cur := h.headBucket.DirectNext(); cur != h.tailBucket; cur = cur.DirectNext() {
		r := bucketFromListHead[StringKey, any](cur).reverse
		if n := len(got); n > 0 && r > got[n-1] {
			return append(got, r), fmt.Errorf("bucket list out of order: %016x after %016x", r, got[n-1])
		}
		got = append(got, r)
		if cur.DirectNext() == cur {
			return got, fmt.Errorf("bucket list stops at the bucket %016x, which is linked to itself", r)
		}
		if len(got) > max {
			return got, fmt.Errorf("bucket list does not reach its tail in %d steps", max)
		}
	}
	return got, nil
}
