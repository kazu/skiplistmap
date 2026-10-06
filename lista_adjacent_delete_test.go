package skiplistmap

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
)

// Two nodes next to each other at the head of a lista_encabezado list are
// deleted at once, while other goroutines insert before the tail: both
// deletes finish, the two nodes are off the list, and the list is whole
// from the head to the tail and back. This is the pattern of two takes of
// the free list that take the head one after the other while puts go on.
func TestListaAdjacentDeletesWithInserts(t *testing.T) {
	const rounds, inserters = 50000, 8
	for round := 0; round < rounds; round++ {
		var head, tail list_head.ListHead
		list_head.InitAsEmpty(&head, &tail)
		nodes := make([]listaListNode, 4+inserters*4)
		for i := range nodes {
			nodes[i].n = i
			list_head.InitAsEmpty(&nodes[i].ListHead, &nodes[i].ListHead)
		}
		for i := 0; i < 4; i++ {
			if _, err := tail.InsertBefore(&nodes[i].ListHead); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(n *list_head.ListHead) {
				defer wg.Done()
				<-start
				for n.MarkForDelete() != nil {
				}
			}(&nodes[i].ListHead)
		}
		for w := 0; w < inserters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := 0; i < 4; i++ {
					n := &nodes[4+w*4+i]
					for {
						if _, err := tail.InsertBefore(&n.ListHead); err == nil {
							break
						}
					}
				}
			}(w)
		}
		close(start)
		wg.Wait()

		// the list from the head: the two deleted nodes are not on it, and
		// every node leads back to the one before it
		seen := map[*list_head.ListHead]bool{}
		prev := &head
		count := 0
		for cur := head.DirectNext(); cur != &tail; cur = cur.DirectNext() {
			if cur == nil || seen[cur] || count > len(nodes) {
				t.Fatalf("round %d: the list from the head loops or breaks at node %d", round, count)
			}
			seen[cur] = true
			if cur == &nodes[0].ListHead || cur == &nodes[1].ListHead {
				t.Fatalf("round %d: deleted node %d is on the list", round, listaNodeOf(cur).n)
			}
			if cur.DirectPrev().WithOutMark() != prev {
				t.Fatalf("round %d: node %d leads back to %p, not to the node before it %p", round, listaNodeOf(cur).n, cur.DirectPrev(), prev)
			}
			prev = cur
			count++
		}
		if tail.DirectPrev().WithOutMark() != prev {
			t.Fatalf("round %d: the tail leads back to %p, not to the last node %p (%s)", round, tail.DirectPrev(), prev, describeLista(&head, &tail))
		}
		if want := 2 + inserters*4; count != want {
			t.Fatalf("round %d: %d nodes on the list, want %d", round, count, want)
		}
	}
}

var listaNodeView = newListaList[listaListNode](unsafe.Offsetof(listaListNode{}.ListHead))

func listaNodeOf(h *list_head.ListHead) *listaListNode {
	return listaNodeView.Element(h)
}

func describeLista(head, tail *list_head.ListHead) string {
	s := ""
	count := 0
	for cur := head; ; cur = cur.DirectNext().WithOutMark() {
		s += fmt.Sprintf("%p ", cur)
		if cur == tail || count > 64 {
			break
		}
		count++
	}
	return s
}
