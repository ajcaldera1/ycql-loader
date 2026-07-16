package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yugabyte/gocql"
)

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

var setupCQL = []string{
	`CREATE TYPE IF NOT EXISTS attribute_source (
		priority          int,
		product_source_id bigint,
		source_id         bigint,
		source_type       text
	)`,
	`CREATE TABLE IF NOT EXISTS retailer_products (
		retailer_id              int,
		product_id               bigint,
		surface_id               int,
		locale_id                int,
		catalog_build_variant_id int,
		attributes               jsonb,
		sources                  map<text, frozen<attribute_source>>,
		created_at               timestamp,
		updated_at               timestamp,
		generated_at             timestamp,
		PRIMARY KEY ((retailer_id, product_id), surface_id, locale_id, catalog_build_variant_id)
	) WITH tablets=4`,
}

// ---------------------------------------------------------------------------
// CLI & main
// ---------------------------------------------------------------------------

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Hosts, "hosts", "127.0.0.1", "Comma-separated YugabyteDB node addresses")
	flag.IntVar(&cfg.Port, "port", 9042, "YCQL native transport port")
	flag.StringVar(&cfg.Keyspace, "keyspace", "demo", "Keyspace name (created if missing)")
	flag.StringVar(&cfg.Username, "username", "cassandra", "YCQL auth username")
	flag.StringVar(&cfg.Password, "password", "cassandra", "YCQL auth password")
	flag.IntVar(&cfg.TotalRows, "rows", 1_000_000, "Total rows to insert")
	flag.IntVar(&cfg.BatchSize, "batch-size", 10, "Rows per UNLOGGED batch (all same partition)")
	flag.IntVar(&cfg.Writers, "writers", 32, "Number of writer goroutines")
	flag.IntVar(&cfg.Generators, "generators", 4, "Number of generator goroutines")
	flag.IntVar(&cfg.RF, "rf", 1, "Replication factor for keyspace creation")
	flag.Parse()

	hosts := parseHosts(cfg.Hosts)

	// Phase 1: bootstrap keyspace + schema (temporary session, no keyspace)
	fmt.Printf("Connecting to %v:%d as '%s' ...\n", hosts, cfg.Port, cfg.Username)
	bootstrap := newSession(hosts, cfg, "")
	createKeyspace(bootstrap, cfg)
	bootstrap.Close()

	// Phase 2: open the working session scoped to the keyspace
	session := newSession(hosts, cfg, cfg.Keyspace)
	defer session.Close()

	createTables(session)

	// Phase 3: discover tablets so we can route batches to the writer that
	// owns each tablet.
	tabletMap, err := DiscoverTablets(session, cfg.Keyspace, "retailer_products")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to discover tablets: %v\n", err)
		os.Exit(1)
	}
	tabletCount := tabletMap.Count()
	if tabletCount == 0 {
		fmt.Fprintf(os.Stderr, "No tablets found in system.partitions for retailer_products\n")
		os.Exit(1)
	}
	effectiveWriters := EffectiveWriters(cfg.Writers, tabletCount)

	fmt.Printf("Discovered %d tablet(s) for retailer_products:\n", tabletCount)
	printTabletRanges(tabletMap)

	// Phase 4: generate + write
	approxMB := cfg.TotalRows * 12 / 1024
	approxPartitions := cfg.TotalRows / CombosPerPartition
	fmt.Printf("Inserting %d rows  |  generators=%d  |  writers=%d (of %d configured)  |  batch_size=%d\n",
		cfg.TotalRows, cfg.Generators, effectiveWriters, cfg.Writers, cfg.BatchSize)
	fmt.Printf("Estimated data volume: ~%d MB  |  ~%d partitions  |  %d tablets\n",
		approxMB, approxPartitions, tabletCount)
	fmt.Printf("Tablet-aware routing: each batch is sent to the writer owning its tablet\n")
	fmt.Println(strings.Repeat("-", 74))

	start := time.Now()

	batches := StartGenerators(cfg.Generators, cfg.TotalRows, cfg.BatchSize, tabletMap)
	StartWriters(cfg.Writers, session, batches, tabletCount, cfg.TotalRows, start)

	elapsed := time.Since(start).Seconds()
	rate := float64(cfg.TotalRows) / elapsed
	fmt.Println(strings.Repeat("-", 74))
	fmt.Printf("Done. %d rows in %.1fs  (%.0f rows/s)\n", cfg.TotalRows, elapsed, rate)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parseHosts(csv string) []string {
	parts := strings.Split(csv, ",")
	hosts := make([]string, 0, len(parts))
	for _, h := range parts {
		if t := strings.TrimSpace(h); t != "" {
			hosts = append(hosts, t)
		}
	}
	return hosts
}

func newCluster(hosts []string, cfg Config, keyspace string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = cfg.Port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.Username,
		Password: cfg.Password,
	}
	cluster.Consistency = gocql.Quorum
	cluster.NumConns = 8
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	if keyspace != "" {
		cluster.Keyspace = keyspace
	}
	return cluster
}

func newSession(hosts []string, cfg Config, keyspace string) *gocql.Session {
	session, err := newCluster(hosts, cfg, keyspace).CreateSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	return session
}

func createKeyspace(session *gocql.Session, cfg Config) {
	q := fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}`,
		cfg.Keyspace, cfg.RF,
	)
	if err := session.Query(q).Exec(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create keyspace: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Using keyspace: %s\n", cfg.Keyspace)
}

func createTables(session *gocql.Session) {
	for _, stmt := range setupCQL {
		if err := session.Query(stmt).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Schema error: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println("Schema ready (type + table).")
}

// printTabletRanges prints each tablet's hash range, collapsing the list to a
// summary when there are many tablets.
func printTabletRanges(tm TabletMap) {
	const maxShown = 8
	for i, t := range tm.Tablets {
		if i >= maxShown {
			fmt.Printf("  ... and %d more\n", len(tm.Tablets)-maxShown)
			break
		}
		fmt.Printf("  tablet %3d  [0x%04x, 0x%04x)  %s\n",
			t.Index, t.StartHash, t.EndHash, t.ID)
	}
}
