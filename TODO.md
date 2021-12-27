- [x] improve performance
  - [x] bucket buffer of donwlevel bucket as slice
  - [x] using elist_head as key/value item
- [ ] support user defined key order.
- [x] refactoring
- [x] fix race condition
  - [x] add/update  in embedded itempool 
  - [x] add/update  in outside itempool
  - [x] global sharedSearchOpt . define atomic.Value
  - [x] SampleItem.SetValue
  - [x] //go:nocheckptr elist_head.Ptr()
  - [x] sp.items = sp.items[:i+1]
  - [x] embedded mode
    - [x] _findBucket -> b.downLevels[idx].level and bucketFromPoolEmbedded -> b.downLevels = b.downLevels[:idx
    - [x] appendLast-> sp.items = sp.items[:olen+1] -> use updateItem() 
    - [s] map.Set() -> s.PtrMapHead().reverse = bits.Reverse64(k)
    - [x] getWithBucket -> e.PtrMapHead().reverse != bits.Reverse64(k) || e.PtrMapHead().conflict != conflict  m.TestSet -> item.PtrMapHead().reverse = bits.Reverse64(k)
    - [x] samepleItemPool.insertToPool() -> copy(newItems[i+1:], sp.items[i:])  SampleItem.SetValue() -> s.V.Store()
    - [x] insertToPool-> updateItems() , map._set() -> item.PtrMapHead().conflict = conflict
    - [x] state4get -> len, cap   updateItems()    
    - [x] map._findBucket -> bucketDowns := b.downLevels , map.bucketFromPoolEmbedded() -> b.downLevels = b.downLevels[:idx+1]
    - [x] samepleItemPool._split sp.items = sp.items[:idx:idx], samepleItemPool.At(). 
        use samepleItemPool.updateWithLock() 
    - [x] map._findBucket() -> if bucketDowns.len <= idx || bucketDowns.at(idx).level == 0 { , h.bucketFromPoolEmbedded() -> b.downLevels = make([]bucket, 1, 16)
        change intialize bucketFromPoolEmbedded() 
    - [x] map.makeBucket2() -> h.bucketFromPoolEmbedded(newReverse) , map._findBucket() -> if bucketDowns.len <= idx || bucketDowns.at(idx).level == 0 {
  - [x] non embedded mode
    - [x] bucketFromPool() -> oBucket.downLevels = oBucket.downLevels[:oIdx+1] and list.at()

- [ ] Implement Purge()
  - [x] basic impleent
  - [x] waiting/locking to traverse on pursing
  - [ ] purge bucket
- [x] impelemnt syncMap()
  - [x] new sync.Map implementaion to re-write dirty as skiplistmap.Map 

- [x] remove not-used function
  - RunLazyUnlocker()
  - map.AddLen() -> addLen()
  - map.GetWithFn()