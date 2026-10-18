// topology.go
package main

// TopologyStrategy 定義所有拓樸規則必須實作的介面
type TopologyStrategy interface {
	ShouldForward(targetPeer string, msg Message) bool
}

// BaseTopology 提供通用的防迴圈基礎規則
type BaseTopology struct {
	gatewayID string
}

func (b *BaseTopology) CommonCheck(targetPeer string, msg Message) bool {
	// Rule 0: 絕不發給自己
	if targetPeer == b.gatewayID {
		return false
	}
	// Rule 1: 絕不發回給「傳訊息給我的那個 Gateway」
	if targetPeer == msg.FromGateway {
		return false
	}
	// Rule 2: 絕不發回給「原始發送訊息的 Gateway」
	if targetPeer == msg.SourceGateway {
		return false
	}
	return true
}
