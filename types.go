package main

import "time"

// AttributeSource mirrors the YCQL UDT attribute_source.
// Field order must match the CREATE TYPE definition.
type AttributeSource struct {
	Priority        int    `cql:"priority"`
	ProductSourceID int64  `cql:"product_source_id"`
	SourceID        int64  `cql:"source_id"`
	SourceType      string `cql:"source_type"`
}

// Row represents a single retailer_products row ready for insertion.
type Row struct {
	RetailerID            int
	ProductID             int64
	SurfaceID             int
	LocaleID              int
	CatalogBuildVariantID int
	Attributes            string
	Sources               map[string]AttributeSource
	CreatedAt             time.Time
	UpdatedAt             time.Time
	GeneratedAt           time.Time
}

// PartitionBatch is a group of rows that share the same partition key
// (retailer_id, product_id) and can be sent in a single UNLOGGED BATCH
// to one tablet. Hash is the 16-bit partition hash of the partition key; it is
// stable regardless of how tablets are currently split, so the coordinator can
// route the batch by looking the hash up against the current tablet map.
type PartitionBatch struct {
	Hash uint16
	Rows []Row
}

// ClusteringCombo holds one combination of clustering column values.
type ClusteringCombo struct {
	SurfaceID int
	LocaleID  int
	VariantID int
}

// AllClusteringCombos enumerates every valid (surface, locale, variant) tuple.
// 5 × 3 × 2 = 30 combinations per partition.
var AllClusteringCombos []ClusteringCombo

func init() {
	for _, s := range []int{1, 2, 3, 4, 5} {
		for _, l := range []int{1, 2, 3} {
			for _, v := range []int{1, 2} {
				AllClusteringCombos = append(AllClusteringCombos, ClusteringCombo{s, l, v})
			}
		}
	}
}

// CombosPerPartition is the total number of clustering combos.
const CombosPerPartition = 30

// Config holds all runtime configuration.
type Config struct {
	Hosts          string
	Port           int
	Keyspace       string
	Username       string
	Password       string
	TotalRows      int
	BatchSize      int
	Writers        int
	Generators     int
	RF             int
	Tablets        int
	DropExisting   bool
	ResyncInterval time.Duration
}
