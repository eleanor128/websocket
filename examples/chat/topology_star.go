// topology_star.go
package main

type StarTopology struct {
	BaseTopology
}

func NewStarTopology(gatewayID string) *StarTopology {
	return &StarTopology{
		BaseTopology: BaseTopology{gatewayID: gatewayID},
	}
}

func (s *StarTopology) ShouldForward(targetPeer string, msg Message) bool {
	// 先執行基礎防迴圈過濾
	if !s.CommonCheck(targetPeer, msg) {
		return false
	}

	// Star 專屬規則：
	// 若我是 Leaf (G1~G3)，訊息是別人傳給我的 (msg.FromGateway != "")，我就不能再往外發
	if s.gatewayID != "G0" && msg.FromGateway != "" {
		return false
	}
	return true
}
