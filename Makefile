SHELL := /bin/bash
.SHELLFLAGS := -e -o pipefail -c

.PHONY: ci vet test race checkptr step

ci: vet test race checkptr step

vet:
	go vet -copylocks=false ./...
	@go list -f '{{.Dir}} {{join .GoFiles " "}}' ./... | while read -r dir files; do \
		if [ -n "$$files" ]; then (cd "$$dir" && go vet -copylocks=true $$files) || exit $$?; fi; \
	done

test:
	go list ./... | xargs -I{} go test -v -count=1 {}

race:
	go list ./... | xargs -I{} go test -v -count=1 -race {}

checkptr:
	go list ./... | xargs -I{} go test -v -count=1 -gcflags=all=-d=checkptr {}

step:
	go test -v -count=1 -race -tags=stephook -run '^(Test|Example|Fuzz)' ./rmap
	go test -v -count=1 -race -tags=stephook -run '^Test_(J(51|56)|OperationsDistinguishSameHashPair|EmbeddedGetDoesNotReadReusedSlot|EmbeddedRangeKeepsKeyAndValueTogether|DifferentKeysWithSameHashPair|F2)|^Test(Typed|EntryCopy|EntryValue|EntryStorage|EntryNext|EntryInitialization|EntryRetention|RegisteredEntry|SearchIntermediatePurged|EmbeddedEntryAccess|EmbeddedExternal|Update|DeletePurge|_PurgeAndSet|_StepRaceSplit|_ConcurrentUpdateWhileGrowing)' .
