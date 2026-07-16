package main

import (
	"context"
	"fmt"
	"time"

	"github.com/yugabyte/gocql"
)

// StartPoller periodically re-queries system.partitions and, whenever the
// tablet layout changes (for example due to automatic tablet splitting), sends
// the new TabletMap on out so the coordinator can realign its writers.
//
// If interval <= 0 the poller is disabled and returns immediately, preserving
// the original static-topology behavior. It runs until ctx is cancelled.
// Transient query errors are logged and skipped so a momentary master
// unavailability never aborts the load.
func StartPoller(
	ctx context.Context,
	session *gocql.Session,
	keyspace, table string,
	interval time.Duration,
	last TabletMap,
	out chan<- TabletMap,
) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm, err := DiscoverTablets(session, keyspace, table)
			if err != nil {
				fmt.Printf("  [resync] warning: failed to query system.partitions: %v\n", err)
				continue
			}
			if nm.Count() == 0 || nm.SameAs(last) {
				continue
			}
			last = nm
			// Deliver the new map, but stay responsive to cancellation in case
			// the coordinator has already finished and stopped reading.
			select {
			case out <- nm:
			case <-ctx.Done():
				return
			}
		}
	}
}
