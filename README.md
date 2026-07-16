# ycql-loader

A high-throughput, tablet-aware bulk data loader for YugabyteDB's YCQL API,
written in Go. It generates large volumes of realistic synthetic data and writes
it in parallel, routing each batch to the writer that owns the target tablet, and
can realign that routing on the fly as YugabyteDB splits tablets during the load.

## Schema

The loader creates (if absent) the following type and table in the target
keyspace:

```sql
CREATE TYPE IF NOT EXISTS attribute_source (
    priority          int,
    product_source_id bigint,
    source_id         bigint,
    source_type       text
);

CREATE TABLE IF NOT EXISTS retailer_products (
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
) WITH tablets=4;
```

The tablet presplit count is configurable via `-tablets`, and the table can be
dropped and recreated with `-drop-existing`.

## Capabilities

- **Realistic synthetic data.** Each row gets a JSONB `attributes` blob averaging
  ~10 KB and a `sources` map of ~150 `attribute_source` UDT entries. Text is
  drawn from a pre-generated word pool for speed.
- **Partition-coherent batching.** Rows are produced one partition key at a time
  (all 30 clustering combinations of a `(retailer_id, product_id)` partition), and
  each `UNLOGGED` batch contains only rows from a single partition so it maps to
  one tablet.
- **Client-side tablet routing.** The partition hash is computed client-side with
  the exact algorithm used by the `yugabyte/gocql` driver (Jenkins hash, seed 97,
  folded into the 16-bit hash space). Tablets and their hash ranges are discovered
  from `system.partitions`, and each batch is routed to the writer that owns its
  tablet so a writer talks to a single tablet where possible.
- **Decoupled generators and writers.** A pool of generator goroutines feeds a
  pool of writer goroutines through channels, so data generation and database I/O
  run concurrently.
- **Dynamic tablet resync (optional).** A background poller can periodically
  re-query `system.partitions`; when tablet boundaries change (for example due to
  automatic tablet splitting), a central coordinator grows or shrinks the writer
  pool and realigns routing live, without interrupting the load. Because every
  batch carries its real partition key, inserts remain correct at all times — a
  resync only restores routing locality.
- **JSONB support.** Uses the YugabyteDB `gocql` fork, which understands the YCQL
  `jsonb` type.
- **Authentication.** Connects with username/password (defaults to the standard
  `cassandra` / `cassandra`).
- **Live progress.** Prints throughput (rows/s), percent complete, and elapsed
  time as it runs, plus `[resync]` lines when the topology realigns.

## Requirements

- Go 1.26+
- A reachable YugabyteDB cluster with the YCQL API enabled (default port 9042)

Dependencies are managed via Go modules; the primary one is
[`github.com/yugabyte/gocql`](https://github.com/yugabyte/gocql).

## Build

```bash
git clone https://github.com/ajcaldera1/ycql-loader.git
cd ycql-loader
go build -o ycql-loader .
```

## Usage

```bash
./ycql-loader [flags]
```

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-hosts` | `127.0.0.1` | Comma-separated YugabyteDB node addresses |
| `-port` | `9042` | YCQL native transport port |
| `-keyspace` | `demo` | Keyspace name (created if missing) |
| `-username` | `cassandra` | YCQL auth username |
| `-password` | `cassandra` | YCQL auth password |
| `-rows` | `1000000` | Total rows to insert |
| `-batch-size` | `10` | Rows per `UNLOGGED` batch (all from one partition) |
| `-writers` | `32` | Number of writer goroutines (capped at the tablet count) |
| `-generators` | `4` | Number of generator goroutines |
| `-rf` | `1` | Replication factor used when creating the keyspace |
| `-tablets` | `4` | Number of tablets to presplit `retailer_products` into when creating it (`<=0` uses the cluster default) |
| `-drop-existing` | `false` | Drop the `retailer_products` table (if present) before creating it |
| `-resync-interval` | `0` | How often to re-query `system.partitions` and realign writers (e.g. `30s`); `0` disables it |

### Examples

Load one million rows into a local single-node cluster with defaults:

```bash
./ycql-loader
```

Load into a multi-node cluster with credentials and higher concurrency:

```bash
./ycql-loader \
  -hosts 10.0.0.1,10.0.0.2,10.0.0.3 \
  -username cassandra \
  -password secret \
  -keyspace demo \
  -rf 3 \
  -rows 5000000 \
  -writers 64 \
  -generators 8
```

Start fresh by dropping any existing table and presplitting into 24 tablets:

```bash
./ycql-loader -drop-existing -tablets 24
```

Enable dynamic tablet resync, checking for tablet splits every 30 seconds:

```bash
./ycql-loader -rows 50000000 -resync-interval 30s
```

## How it works

```
generators ──▶ genOut channel ──▶ coordinator ──▶ per-writer channels ──▶ writers ──▶ YCQL
                                       ▲
                                       │ new tablet map
                                    poller (every -resync-interval)
```

1. The keyspace, UDT, and table are created if needed (optionally dropping an
   existing table first, and presplitting into a configurable number of tablets).
2. Tablets and their hash ranges are discovered from `system.partitions`.
3. Generators produce partition-coherent batches, each tagged with its stable
   partition hash.
4. The coordinator looks up the owning tablet for each batch and routes it to the
   writer responsible for that tablet.
5. If `-resync-interval` is set, the poller detects tablet-boundary changes and
   the coordinator realigns the writer pool live.

## Notes

- Tablet-aware routing is a locality optimization; correctness of inserts never
  depends on it, since each write carries its full partition key.
- The table is created presplit into `-tablets` tablets (default 4); adjust as
  needed for your cluster sizing or to exercise the resync path alongside a lower
  split threshold. Use `-drop-existing` to recreate the table from scratch (for
  example when changing the tablet count of an existing table).
