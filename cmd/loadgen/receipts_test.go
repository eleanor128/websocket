package main

import (
	"sync"
	"testing"
)

func TestReceiverJournalsConcurrentAccounting(t *testing.T) {
	const clients, events = 200, 1000
	journals := make([]*receiverJournal, clients)
	for i := range journals {
		journals[i] = newReceiverJournal(events)
	}
	var wg sync.WaitGroup
	for _, journal := range journals {
		wg.Add(1)
		go func(j *receiverJournal) {
			defer wg.Done()
			// Deliberately out of order; the last event remains missing.
			for event := events - 2; event >= 0; event-- {
				j.record(event, int64(event))
			}
			for event := 0; event < events-1; event++ {
				j.record(event, int64(event+5000))
			}
		}(journal)
	}
	wg.Wait()
	for _, j := range journals {
		for event, s := range j.receipts {
			if event == events-1 {
				if s.Copies != 0 {
					t.Fatal("missing delivery counted as present")
				}
			} else if s.Copies != 2 || s.At != int64(event) {
				t.Fatalf("first receipt/duplicate accounting changed: event %d: %+v", event, s)
			}
		}
	}
}

// Receipt storage previously allocated an object and expanded shared maps on
// every first delivery. At 200 clients x 9,722 messages that is nearly two
// million hot-path allocations; recording now requires none after setup.
func TestReceiverJournalRecordingAllocations(t *testing.T) {
	j := newReceiverJournal(9722)
	allocs := testing.AllocsPerRun(3, func() {
		for i := range j.receipts {
			j.receipts[i] = receiptStamp{}
		}
		for i := range j.receipts {
			j.record(i, int64(i))
		}
	})
	if allocs != 0 {
		t.Fatalf("recording allocates %.0f objects per trace", allocs)
	}
}
