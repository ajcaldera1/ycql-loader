package main

import (
	"encoding/binary"
	"sort"

	"github.com/yugabyte/gocql"
)

// ---------------------------------------------------------------------------
// Partition hash — ported from yugabyte/gocql ybdb_hash.go
//
// YCQL uses a Jenkins hash variant (seed 97) over the serialized partition
// key, folded into the 16-bit hash space [0, 65535]. This mirrors the
// partition_hash() built-in and the driver's own routing logic, so the value
// we compute here matches the tablet the driver would target.
// ---------------------------------------------------------------------------

func getByte(k []byte, pos int) int64 {
	return int64(k[pos]) & int64(0xff)
}

func getLong(k []byte, pos int) int64 {
	return getByte(k, pos) |
		getByte(k, pos+1)<<8 |
		getByte(k, pos+2)<<16 |
		getByte(k, pos+3)<<24 |
		getByte(k, pos+4)<<32 |
		getByte(k, pos+5)<<40 |
		getByte(k, pos+6)<<48 |
		getByte(k, pos+7)<<56
}

func unsignedRightShift(n int64, p int64) int64 {
	return int64(uint64(n) >> p)
}

func hash64(k []byte, s int64) int64 {
	var golden uint64 = 0xe08c1d668b756f82 // the golden ratio, an arbitrary value
	a := int64(golden)
	b := int64(golden)
	c := s

	pos := 0

	for len(k)-pos >= 24 {
		a += getLong(k, pos)
		pos += 8
		b += getLong(k, pos)
		pos += 8
		c += getLong(k, pos)
		pos += 8

		a, b, c = mix64(a, b, c)
	}

	c += int64(len(k))
	switch len(k) - pos {
	case 23:
		c += getByte(k, pos+22) << 56
		fallthrough
	case 22:
		c += getByte(k, pos+21) << 48
		fallthrough
	case 21:
		c += getByte(k, pos+20) << 40
		fallthrough
	case 20:
		c += getByte(k, pos+19) << 32
		fallthrough
	case 19:
		c += getByte(k, pos+18) << 24
		fallthrough
	case 18:
		c += getByte(k, pos+17) << 16
		fallthrough
	case 17:
		c += getByte(k, pos+16) << 8
		fallthrough
	case 16:
		b += getLong(k, pos+8)
		a += getLong(k, pos)
	case 15:
		b += getByte(k, pos+14) << 48
		fallthrough
	case 14:
		b += getByte(k, pos+13) << 40
		fallthrough
	case 13:
		b += getByte(k, pos+12) << 32
		fallthrough
	case 12:
		b += getByte(k, pos+11) << 24
		fallthrough
	case 11:
		b += getByte(k, pos+10) << 16
		fallthrough
	case 10:
		b += getByte(k, pos+9) << 8
		fallthrough
	case 9:
		b += getByte(k, pos+8)
		fallthrough
	case 8:
		a += getLong(k, pos)
	case 7:
		a += getByte(k, pos+6) << 48
		fallthrough
	case 6:
		a += getByte(k, pos+5) << 40
		fallthrough
	case 5:
		a += getByte(k, pos+4) << 32
		fallthrough
	case 4:
		a += getByte(k, pos+3) << 24
		fallthrough
	case 3:
		a += getByte(k, pos+2) << 16
		fallthrough
	case 2:
		a += getByte(k, pos+1) << 8
		fallthrough
	case 1:
		a += getByte(k, pos)
	}

	_, _, c = mix64(a, b, c)
	return c
}

// mix64 is the Jenkins 64-bit mixing step.
func mix64(a, b, c int64) (int64, int64, int64) {
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 43)
	b -= c
	b -= a
	b ^= a << 9
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 8)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 38)
	b -= c
	b -= a
	b ^= a << 23
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 5)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 35)
	b -= c
	b -= a
	b ^= a << 49
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 11)
	a -= b
	a -= c
	a ^= unsignedRightShift(c, 12)
	b -= c
	b -= a
	b ^= a << 18
	c -= a
	c -= b
	c ^= unsignedRightShift(b, 22)
	return a, b, c
}

// getKey folds the 64-bit hash into the 16-bit YCQL partition hash space.
func getKey(b []byte) int64 {
	const seed int64 = 97
	h := hash64(b, seed)
	h1 := unsignedRightShift(h, 48)
	h2 := 3 * unsignedRightShift(h, 32)
	h3 := 5 * unsignedRightShift(h, 16)
	h4 := 7 * (h & 0xffff)
	return (h1 ^ h2 ^ h3 ^ h4) & 0xffff
}

// EncodePartitionKey serializes (retailer_id int, product_id bigint) exactly
// as the driver's createRoutingKeyYb does: a plain concatenation of the CQL
// big-endian encodings (4 bytes for int, 8 bytes for bigint), with no length
// prefixes or separators.
func EncodePartitionKey(retailerID int, productID int64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b[0:4], uint32(int32(retailerID)))
	binary.BigEndian.PutUint64(b[4:12], uint64(productID))
	return b
}

// PartitionHash returns the 16-bit YCQL partition hash for a partition key.
func PartitionHash(retailerID int, productID int64) uint16 {
	return uint16(getKey(EncodePartitionKey(retailerID, productID)))
}

// ---------------------------------------------------------------------------
// Tablet discovery + routing
// ---------------------------------------------------------------------------

// hashSpaceMax is the exclusive upper bound of the YCQL hash space.
const hashSpaceMax int64 = 65536

// Tablet describes one tablet's ownership of a contiguous hash range
// [StartHash, EndHash).
type Tablet struct {
	Index     int
	StartHash int64
	EndHash   int64
	ID        string
}

// TabletMap holds tablets sorted by StartHash for fast lookup.
type TabletMap struct {
	Tablets []Tablet
}

// Count returns the number of tablets.
func (tm TabletMap) Count() int {
	return len(tm.Tablets)
}

// Lookup returns the index of the tablet that owns the given partition hash.
func (tm TabletMap) Lookup(hash uint16) int {
	h := int64(hash)
	// Find the first tablet whose StartHash is greater than h; the tablet we
	// want is the one immediately before it.
	i := sort.Search(len(tm.Tablets), func(i int) bool {
		return tm.Tablets[i].StartHash > h
	})
	if i == 0 {
		return tm.Tablets[0].Index
	}
	return tm.Tablets[i-1].Index
}

// hashFromKeyBytes interprets a system.partitions start/end key (big-endian
// bytes) as an int64. An empty key means the boundary of the hash space.
func hashFromKeyBytes(b []byte, empty int64) int64 {
	if len(b) == 0 {
		return empty
	}
	var v int64
	for _, by := range b {
		v = (v << 8) | (int64(by) & 0xff)
	}
	return v
}

// DiscoverTablets queries system.partitions to enumerate the tablets of a
// table and their hash ranges, returning a sorted TabletMap.
func DiscoverTablets(session *gocql.Session, keyspace, table string) (TabletMap, error) {
	iter := session.Query(
		"SELECT start_key, end_key, id FROM system.partitions WHERE keyspace_name = ? AND table_name = ?",
		keyspace, table,
	).Iter()

	var (
		startKey []byte
		endKey   []byte
		id       gocql.UUID
	)
	var tablets []Tablet
	for iter.Scan(&startKey, &endKey, &id) {
		tablets = append(tablets, Tablet{
			StartHash: hashFromKeyBytes(startKey, 0),
			EndHash:   hashFromKeyBytes(endKey, hashSpaceMax),
			ID:        id.String(),
		})
	}
	if err := iter.Close(); err != nil {
		return TabletMap{}, err
	}

	sort.Slice(tablets, func(i, j int) bool {
		return tablets[i].StartHash < tablets[j].StartHash
	})
	for i := range tablets {
		tablets[i].Index = i
	}

	return TabletMap{Tablets: tablets}, nil
}
