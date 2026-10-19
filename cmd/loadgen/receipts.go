package main

// Each WebSocket has exactly one reader. It exclusively owns this journal
// until readers.Wait returns, so recording never contends with another reader
// or the publication scheduler. Message/client strings live in the trace once.
type receiptStamp struct {
	At     int64
	Copies int
}

type receiverJournal struct {
	receipts   []receiptStamp
	unexpected map[string]int
	disconnect string
}

func newReceiverJournal(events int) *receiverJournal {
	return &receiverJournal{receipts: make([]receiptStamp, events), unexpected: make(map[string]int)}
}

func (r *receiverJournal) record(event int, at int64) {
	s := &r.receipts[event]
	if s.Copies == 0 {
		s.At = at
	}
	s.Copies++
}
