// topology_direct.go
package main

type DirectTopology struct {
	BaseTopology
}

func NewDirectTopology(gatewayID string) *DirectTopology {
	return &DirectTopology{
		BaseTopology: BaseTopology{gatewayID: gatewayID},
	}
}

func (d *DirectTopology) ShouldForward(targetPeer string, msg Message) bool {
	if !validGatewayID(d.gatewayID) || !validGatewayID(targetPeer) || !d.CommonCheck(targetPeer, msg) {
		return false
	}

	// Direct 專屬規則：只有原始 Gateway 會直連所有 Peer 廣播一次
	if msg.FromGateway != "" || msg.SourceGateway != d.gatewayID {
		return false
	}
	return true
}
