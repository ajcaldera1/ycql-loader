package main

import (
	"context"
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

const createTypeCQL = `CREATE TYPE IF NOT EXISTS attribute_source (
	priority          int,
	product_source_id bigint,
	source_id         bigint,
	source_type       text
)`

const dropTableCQL = `DROP TABLE IF EXISTS retailer_products`

// createTableCQL builds the retailer_products DDL, presplitting into the given
// number of tablets. A tablet count <= 0 omits the WITH tablets clause and lets
// the cluster choose the default.
func createTableCQL(tablets int) string {
	base := `CREATE TABLE IF NOT EXISTS retailer_products (
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
	)`
	if tablets > 0 {
		return fmt.Sprintf("%s WITH tablets=%d", base, tablets)
	}
	return base
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
	flag.IntVar(&cfg.Tablets, "tablets", 4,
		"Number of tablets to presplit retailer_products into when creating it (<=0 = cluster default)")
	flag.BoolVar(&cfg.DropExisting, "drop-existing", false,
		"Drop the retailer_products table if it already exists before creating it")
	flag.DurationVar(&cfg.ResyncInterval, "resync-interval", 0,
		"How often to re-query system.partitions and realign writers to new tablet boundaries (e.g. 30s; 0 = disabled)")
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

	createTables(session, cfg)

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

	// Phase 4: start the resync poller (no-op when interval is 0) so writers
	// realign as tablets split during the load.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newMapCh := make(chan TabletMap, 1)
	go StartPoller(ctx, session, cfg.Keyspace, "retailer_products", cfg.ResyncInterval, tabletMap, newMapCh)

	resyncDesc := "disabled"
	if cfg.ResyncInterval > 0 {
		resyncDesc = cfg.ResyncInterval.String()
	}

	// Phase 5: generate + write
	approxMB := cfg.TotalRows * 12 / 1024
	approxPartitions := cfg.TotalRows / CombosPerPartition
	fmt.Printf("Inserting %d rows  |  generators=%d  |  writers=%d (of %d configured)  |  batch_size=%d\n",
		cfg.TotalRows, cfg.Generators, effectiveWriters, cfg.Writers, cfg.BatchSize)
	fmt.Printf("Estimated data volume: ~%d MB  |  ~%d partitions  |  %d tablets\n",
		approxMB, approxPartitions, tabletCount)
	fmt.Printf("Tablet-aware routing: each batch is sent to the writer owning its tablet\n")
	fmt.Printf("Tablet resync: %s\n", resyncDesc)
	fmt.Println(strings.Repeat("-", 74))

	start := time.Now()

	batches := StartGenerators(cfg.Generators, cfg.TotalRows, cfg.BatchSize)
	RunCoordinator(ctx, session, batches, newMapCh, tabletMap, cfg, cfg.TotalRows, start)

	// Generation and writes are done; stop the poller.
	cancel()

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

func createTables(session *gocql.Session, cfg Config) {
	if cfg.DropExisting {
		if err := session.Query(dropTableCQL).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to drop retailer_products: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Dropped existing retailer_products table.")
	}

	stmts := []string{createTypeCQL, createTableCQL(cfg.Tablets)}
	for _, stmt := range stmts {
		if err := session.Query(stmt).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Schema error: %v\n", err)
			os.Exit(1)
		}
	}
	if cfg.Tablets > 0 {
		fmt.Printf("Schema ready (type + table, presplit into %d tablets).\n", cfg.Tablets)
	} else {
		fmt.Println("Schema ready (type + table, cluster-default tablets).")
	}
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
