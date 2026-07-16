package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yugabyte/gocql"
)

const insertCQL = `INSERT INTO retailer_products (
	retailer_id, product_id, surface_id, locale_id, catalog_build_variant_id,
	attributes, sources, created_at, updated_at, generated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// EffectiveWriters returns how many writer goroutines will actually run: the
// configured count, capped at the number of tablets (no point having more
// writers than tablets to serve).
func EffectiveWriters(configured, tabletCount int) int {
	if tabletCount > 0 && configured > tabletCount {
		return tabletCount
	}
	return configured
}

// StartWriters wires up tablet-aware routing and blocks until every batch from
// the generator channel has been written.
//
// Design: there is one input channel per writer. Each batch is routed to a
// writer by mapping its tablet index onto a writer (tabletIdx % numWriters),
// so all batches for a given tablet always land on the same writer and no
// tablet is ever split across writers. When numWriters == tabletCount this is
// exactly one writer per tablet; when there are more tablets than writers the
// tablets are distributed round-robin, each writer owning a fixed subset. Each
// writer drains exactly one channel, which avoids the deadlock that draining
// multiple still-open channels sequentially would cause.
func StartWriters(
	configuredWriters int,
	session *gocql.Session,
	batches <-chan PartitionBatch,
	tabletCount int,
	totalRows int,
	startTime time.Time,
) {
	numWriters := EffectiveWriters(configuredWriters, tabletCount)
	if numWriters < 1 {
		numWriters = 1
	}

	// One buffered channel per writer.
	writerChans := make([]chan PartitionBatch, numWriters)
	for i := range writerChans {
		writerChans[i] = make(chan PartitionBatch, 16)
	}

	// Router: fan the generator output out to per-writer channels based on the
	// owning tablet, then close all writer channels once generation is done.
	go func() {
		for pb := range batches {
			writerChans[pb.TabletIdx%numWriters] <- pb
		}
		for i := range writerChans {
			close(writerChans[i])
		}
	}()

	var written atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(in <-chan PartitionBatch) {
			defer wg.Done()
			for pb := range in {
				if err := writeBatch(session, pb); err != nil {
					fmt.Printf("  ERROR writing batch: %v\n", err)
					continue
				}
				done := written.Add(int64(len(pb.Rows)))
				reportProgress(done, int64(totalRows), startTime)
			}
		}(writerChans[w])
	}

	wg.Wait()
}

func writeBatch(session *gocql.Session, pb PartitionBatch) error {
	batch := session.NewBatch(gocql.UnloggedBatch)
	for _, r := range pb.Rows {
		batch.Query(insertCQL,
			r.RetailerID,
			r.ProductID,
			r.SurfaceID,
			r.LocaleID,
			r.CatalogBuildVariantID,
			r.Attributes,
			r.Sources,
			r.CreatedAt,
			r.UpdatedAt,
			r.GeneratedAt,
		)
	}
	return session.ExecuteBatch(batch)
}

var lastReportedPct atomic.Int64

func reportProgress(done, total int64, start time.Time) {
	pct := done * 100 / total
	// Report at every 1% increment
	prev := lastReportedPct.Load()
	if pct > prev && lastReportedPct.CompareAndSwap(prev, pct) {
		elapsed := time.Since(start).Seconds()
		rate := float64(done) / elapsed
		fmt.Printf("  %10d / %d  (%3d%%)  %.0f rows/s  elapsed %.0fs\n",
			done, total, pct, rate, elapsed)
	}
}
