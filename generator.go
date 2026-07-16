package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Word pool — pre-built once at init for fast random text generation
// ---------------------------------------------------------------------------

const wordPoolSize = 5000

var wordPool []string

func init() {
	wordPool = make([]string, wordPoolSize)
	for i := range wordPool {
		wlen := 3 + rand.Intn(10) // 3–12 chars
		b := make([]byte, wlen)
		for j := range b {
			b[j] = 'a' + byte(rand.Intn(26))
		}
		wordPool[i] = string(b)
	}
}

var attributeKeys = []string{
	"title", "description", "brand", "category", "subcategory",
	"color", "size", "weight", "material", "upc", "sku", "gtin",
	"model_number", "manufacturer", "country_of_origin", "dimensions",
	"warranty_info", "care_instructions", "features", "specifications",
	"nutrition_facts", "ingredients", "allergens", "certifications",
	"ratings_summary", "review_highlights", "seo_title", "seo_description",
	"seo_keywords", "custom_label_0", "custom_label_1", "custom_label_2",
	"promotion_text", "availability_note", "shipping_info", "return_policy",
	"age_group", "gender", "condition", "energy_rating",
}

var sourceTypes = []string{
	"feed", "manual", "enrichment", "catalog", "scrape", "api", "import",
}

const numRetailers = 200

// ---------------------------------------------------------------------------
// Text / data helpers
// ---------------------------------------------------------------------------

func randText(rng *rand.Rand, length int) string {
	var sb strings.Builder
	sb.Grow(length)
	for sb.Len() < length {
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(wordPool[rng.Intn(wordPoolSize)])
	}
	s := sb.String()
	if len(s) > length {
		return s[:length]
	}
	return s
}

func generateAttributesJSON(rng *rand.Rand, targetBytes int) string {
	obj := make(map[string]string, len(attributeKeys))
	perKey := targetBytes / len(attributeKeys)
	for _, key := range attributeKeys {
		variation := rng.Intn(41) - 20 // -20 to +20
		l := perKey + variation
		if l < 10 {
			l = 10
		}
		obj[key] = randText(rng, l)
	}
	b, _ := json.Marshal(obj)
	return string(b)
}

func generateSources(rng *rand.Rand, count int) map[string]AttributeSource {
	sources := make(map[string]AttributeSource, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("source_%04d", i)
		sources[key] = AttributeSource{
			Priority:        rng.Intn(100) + 1,
			ProductSourceID: rng.Int63n(1_000_000_000_000) + 1,
			SourceID:        rng.Int63n(1_000_000_000) + 1,
			SourceType:      sourceTypes[rng.Intn(len(sourceTypes))],
		}
	}
	return sources
}

// ---------------------------------------------------------------------------
// Partition row builder
// ---------------------------------------------------------------------------

func makePartitionRows(rng *rand.Rand, retailerID int, productID int64, combos []ClusteringCombo) []Row {
	now := time.Now().UTC()
	rows := make([]Row, 0, len(combos))
	for _, c := range combos {
		numSources := 120 + rng.Intn(61) // 120–180
		attrSize := 8000 + rng.Intn(4001) // 8000–12000
		rows = append(rows, Row{
			RetailerID:            retailerID,
			ProductID:             productID,
			SurfaceID:             c.SurfaceID,
			LocaleID:              c.LocaleID,
			CatalogBuildVariantID: c.VariantID,
			Attributes:            generateAttributesJSON(rng, attrSize),
			Sources:               generateSources(rng, numSources),
			CreatedAt:             now,
			UpdatedAt:             now,
			GeneratedAt:           now,
		})
	}
	return rows
}

// ---------------------------------------------------------------------------
// Generator — produces PartitionBatch items on the output channel
// ---------------------------------------------------------------------------

// StartGenerators launches n goroutines that collectively produce totalRows
// worth of PartitionBatch items on the returned channel. Each batch contains
// up to batchSize rows, all sharing one partition key. Each batch is tagged
// with the index of the tablet that owns its partition, computed from the
// client-side partition hash.
func StartGenerators(n, totalRows, batchSize int, tabletMap TabletMap) <-chan PartitionBatch {
	ch := make(chan PartitionBatch, n*4)

	rowsPerGen := totalRows / n
	remainder := totalRows % n

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		quota := rowsPerGen
		if i < remainder {
			quota++
		}
		wg.Add(1)
		go func(quota int, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			emitted := 0

			for emitted < quota {
				retailerID := rng.Intn(numRetailers) + 1
				productID := rng.Int63n(1_000_000_000_000) + 1

				// Determine which tablet owns this partition key.
				hash := PartitionHash(retailerID, productID)
				tabletIdx := tabletMap.Lookup(hash)

				remaining := quota - emitted
				nCombos := CombosPerPartition
				if remaining < nCombos {
					nCombos = remaining
				}

				// Shuffle and pick nCombos clustering combos
				combos := make([]ClusteringCombo, len(AllClusteringCombos))
				copy(combos, AllClusteringCombos)
				rng.Shuffle(len(combos), func(a, b int) {
					combos[a], combos[b] = combos[b], combos[a]
				})
				combos = combos[:nCombos]

				rows := makePartitionRows(rng, retailerID, productID, combos)

				// Slice into batches of batchSize, all same partition/tablet
				for j := 0; j < len(rows); j += batchSize {
					end := j + batchSize
					if end > len(rows) {
						end = len(rows)
					}
					ch <- PartitionBatch{TabletIdx: tabletIdx, Rows: rows[j:end]}
				}
				emitted += len(rows)
			}
		}(quota, int64(i)+time.Now().UnixNano())
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}
