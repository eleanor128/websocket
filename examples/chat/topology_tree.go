// topology_tree.go
package main

type TreeTopology struct {
	BaseTopology
}

func NewTreeTopology(gatewayID string) *TreeTopology {
	return &TreeTopology{
		BaseTopology: BaseTopology{gatewayID: gatewayID},
	}
}

func (t *TreeTopology) ShouldForward(targetPeer string, msg Message) bool {
	if !t.CommonCheck(targetPeer, msg) {
		return false
	}
	// Tree 專屬規則：順著樹狀結構廣播給非來向的鄰居
	return true
}
