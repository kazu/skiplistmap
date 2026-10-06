package skiplistmap

import (
	"sync"
	"testing"

	list_head "github.com/kazu/lista_encabezado"
)

// The only node of a lista_encabezado list is deleted while two goroutines
// insert before the tail, whose previous node that node is: the delete
// finishes, the node is off the list, both inserted nodes are on it, and
// the list is whole. This is a take of the last array of the free list
// while two puts go on.
func TestListaDeleteLastNodeWithInserts(t *testing.T) {
	const rounds, inserters = 50000, 2
	for round := 0; round < rounds; round++ {
		var head, tail list_head.ListHead
		list_head.InitAsEmpty(&head, &tail)
		nodes := make([]listaListNode, 1+inserters)
		for i := range nodes {
			nodes[i].n = i
			list_head.InitAsEmpty(&nodes[i].ListHead, &nodes[i].ListHead)
		}
		if _, err := tail.InsertBefore(&nodes[0].ListHead); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for nodes[0].ListHead.MarkForDelete() != nil {
			}
		}()
		for w := 0; w < inserters; w++ {
			wg.Add(1)
			go func(n *listaListNode) {
				defer wg.Done()
				<-start
				for {
					if _, err := tail.InsertBefore(&n.ListHead); err == nil {
						return
					}
				}
			}(&nodes[1+w])
		}
		close(start)
		wg.Wait()

		seen := map[*list_head.ListHead]bool{}
		prev := &head
		count := 0
		for cur := head.DirectNext().WithOutMark(); cur != &tail; cur = cur.DirectNext().WithOutMark() {
			if seen[cur] || count > len(nodes) {
				t.Fatalf("round %d: the list from the head loops: %s", round, describeLista(&head, &tail))
			}
			seen[cur] = true
			if cur == &nodes[0].ListHead {
				t.Fatalf("round %d: the deleted node is on the list: %s", round, describeLista(&head, &tail))
			}
			if cur.DirectPrev().WithOutMark() != prev {
				t.Fatalf("round %d: node %d leads back to %p, not to %p: %s", round, listaNodeOf(cur).n, cur.DirectPrev(), prev, describeLista(&head, &tail))
			}
			prev = cur
			count++
		}
		if tail.DirectPrev().WithOutMark() != prev {
			t.Fatalf("round %d: the tail leads back to %p, not to the last node %p: %s", round, tail.DirectPrev(), prev, describeLista(&head, &tail))
		}
		if count != inserters {
			t.Fatalf("round %d: %d nodes on the list, want %d", round, count, inserters)
		}
	}
}
