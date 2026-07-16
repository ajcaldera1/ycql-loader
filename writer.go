package main

import (
	"context"
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

// RunCoordinator owns all mutable routing topology (the current tablet map, the
// writer channels, and the writer goroutines) and blocks until every batch from
// the generator channel has been written.
//
// Design: a single goroutine (this function) is the sole owner of the topology,
// so reconfiguration needs no locks. It runs one select loop over incoming
// batches and new tablet maps from the poller. Each batch carries a stable
// partition hash; the coordinator looks it up against the current tablet map and
// routes it to a writer via tabletIdx % numWriters, so all batches for a tablet
// land on the same writer and no tablet is split across writers.
//
// When a resync reports more tablets, the writer pool grows (up to the
// configured max); when it reports fewer, the pool shrinks. Because every batch
// still carries its real partition key, YugabyteDB routes each insert correctly
// regardless of which writer sends it, so a stale route during the brief
// reconfiguration window costs only locality, never correctness.
func RunCoordinator(
	ctx context.Context,
	session *gocql.Session,
	genOut <-chan PartitionBatch,
	newMapCh <-chan TabletMap,
	initial TabletMap,
	cfg Config,
	totalRows int,
	start time.Time,
) {
	var written atomic.Int64
	var wg sync.WaitGroup

	// A writer drains exactly one channel, writing each batch to YCQL.
	startWriter := func(in <-chan PartitionBatch) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pb := range in {
				if err := writeBatch(session, pb); err != nil {
					fmt.Printf("  ERROR writing batch: %v\n", err)
					continue
				}
				done := written.Add(int64(len(pb.Rows)))
				reportProgress(done, int64(totalRows), start)
			}
		}()
	}

	cur := initial
	numWriters := EffectiveWriters(cfg.Writers, cur.Count())
	if numWriters < 1 {
		numWriters = 1
	}

	writerChans := make([]chan PartitionBatch, numWriters)
	for i := range writerChans {
		writerChans[i] = make(chan PartitionBatch, 16)
		startWriter(writerChans[i])
	}

	// reconfigure realigns the writer pool onto a new tablet layout.
	reconfigure := func(nm TabletMap) {
		if nm.Count() == 0 {
			return
		}
		oldTablets := cur.Count()
		oldNum := numWriters

		newNum := EffectiveWriters(cfg.Writers, nm.Count())
		if newNum < 1 {
			newNum = 1
		}

		switch {
		case newNum > oldNum:
			for i := oldNum; i < newNum; i++ {
				ch := make(chan PartitionBatch, 16)
				writerChans = append(writerChans, ch)
				startWriter(ch)
			}
		case newNum < oldNum:
			for i := newNum; i < oldNum; i++ {
				close(writerChans[i])
			}
			writerChans = writerChans[:newNum]
		}

		cur = nm
		numWriters = newNum

		fmt.Printf("  [resync] tablets %d -> %d, writers %d -> %d\n",
			oldTablets, nm.Count(), oldNum, newNum)
	}

loop:
	for {
		select {
		case pb, ok := <-genOut:
			if !ok {
				break loop
			}
			w := cur.Lookup(pb.Hash) % numWriters
			writerChans[w] <- pb
		case nm := <-newMapCh:
			reconfigure(nm)
		case <-ctx.Done():
			break loop
		}
	}

	// Generation finished (or shutdown requested): close all writer channels
	// and wait for in-flight writes to drain.
	for i := range writerChans {
		close(writerChans[i])
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
