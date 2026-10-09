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

// Tree edges: G0-G1, G0-G2, G1-G3 (undirected/bidirectional).
func treeEdge(a, b string) bool {
	return (a == "G0" && (b == "G1" || b == "G2")) ||
		(a == "G1" && (b == "G0" || b == "G3")) ||
		(a == "G2" && b == "G0") ||
		(a == "G3" && b == "G1")
}

func expectedTreePeers(id string) []string {
	switch id {
	case "G0":
		return []string{"G1", "G2"}
	case "G1":
		return []string{"G0", "G3"}
	case "G2":
		return []string{"G0"}
	case "G3":
		return []string{"G1"}
	default:
		return nil
	}
}

func (t *TreeTopology) ShouldForward(targetPeer string, msg Message) bool {
	if !validGatewayID(t.gatewayID) || !validGatewayID(targetPeer) {
		return false
	}
	// Must be an edge in the tree.
	if !treeEdge(t.gatewayID, targetPeer) {
		return false
	}
	// Common anti-loop checks:
	// Rule 0: targetPeer != t.gatewayID
	// Rule 1: targetPeer != msg.FromGateway (never send back along incoming edge)
	// Rule 2: targetPeer != msg.SourceGateway (never send back to original source)
	if !t.CommonCheck(targetPeer, msg) {
		return false
	}

	return true
}
