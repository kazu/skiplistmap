package skiplistmap

import (
	"testing"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
)

type listaListNode struct {
	n int
	list_head.ListHead
}

// A listaList links, walks and unlinks elements through the ListHead
// embedded in them, and gives the element back for its link.
func TestListaListLinksElements(t *testing.T) {
	var head, tail list_head.ListHead
	list_head.InitAsEmpty(&head, &tail)
	l := newListaList[listaListNode](unsafe.Offsetof(listaListNode{}.ListHead))
	nodes := make([]listaListNode, 3)
	for i := range nodes {
		nodes[i].n = i
		list_head.InitAsEmpty(&nodes[i].ListHead, &nodes[i].ListHead)
		if err := l.InsertBefore(&tail, &nodes[i]); err != nil {
			t.Fatal(err)
		}
	}
	if l.Element(head.Next()) != &nodes[0] || l.Next(&nodes[0]) != &nodes[1] || l.Prev(&nodes[2]) != &nodes[1] {
		t.Fatal("the elements are not in the order of their insertion")
	}
	if l.Link(&nodes[1]) != &nodes[1].ListHead {
		t.Fatal("Link is not the embedded ListHead")
	}
	if err := l.MarkForDelete(&nodes[1]); err != nil {
		t.Fatal(err)
	}
	if l.Next(&nodes[0]) != &nodes[2] || l.Prev(&nodes[2]) != &nodes[0] {
		t.Fatal("the unlinked element is still between its neighbours")
	}
}
