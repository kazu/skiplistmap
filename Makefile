SHELL := /bin/bash
.SHELLFLAGS := -e -o pipefail -c

GO_TEST_FLAGS := -race
STRESS_TESTS := ^(Test_(SetSequential|SetWithForcedGC|ConcurrentReadUpdate|RangeVisitsAll|ConcurrentDeleteReinsert|DeleteReinsert|HMap|SearchDuringSplit|ConcurrentNewKeys|ConcurrentStoreItem|ConcurrentUpdateWhileGrowing|ConcurrentDeleteWhileGrowing|PurgeAndSetManyTimes|PurgeAndSetFewKeysManyTimes|LoadItemAfterPoolGrowth)|TestEntryCopyRetainedHeap)$$

.PHONY: ci vet test step stress

ci: vet test step

vet:
	go vet -copylocks=false ./...
	@go list -f '{{.Dir}} {{join .GoFiles " "}}' ./... | while read -r dir files; do \
		if [ -n "$$files" ]; then (cd "$$dir" && go vet -copylocks=true $$files) || exit $$?; fi; \
	done

test:
	go list ./... | xargs -I{} go test -v $(GO_TEST_FLAGS) -skip '$(STRESS_TESTS)' {}

step:
	go test -v $(GO_TEST_FLAGS) -tags=stephook -run '^(Test|Example|Fuzz)' ./rmap
	go test -v $(GO_TEST_FLAGS) -tags=stephook -skip '$(STRESS_TESTS)' -run '^Test_(J(51|56)|OperationsDistinguishSameHashPair|EmbeddedGetDoesNotReadReusedSlot|EmbeddedRangeKeepsKeyAndValueTogether|DifferentKeysWithSameHashPair|F2)|^Test(PoolInsert|FreePools|Typed|EntryCopy|EntryValue|EntryStorage|EntryNext|EntryInitialization|EntryRetention|RegisteredEntry|SearchIntermediatePurged|EmbeddedEntryAccess|EmbeddedExternal|Update|DeletePurge|_PurgeAndSet|_StepRaceSplit|_ConcurrentUpdateWhileGrowing)' .

stress:
	go test -v $(GO_TEST_FLAGS) -timeout=60m -run '$(STRESS_TESTS)' .
	go test -v $(GO_TEST_FLAGS) -tags=stephook -run '^Test_ConcurrentUpdateWhileGrowing$$' .
