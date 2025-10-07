package store

import (
	"fmt"
	"godb/internal/schema"
	"os"
	"testing"
)

// Helper to create test schema
func benchSchema() schema.Schema {
	return schema.Schema{
		TableName: "bench",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "name", Type: schema.StringType},
			{Name: "value", Type: schema.FloatType},
		},
	}
}

// Helper to create test record
func benchRecord(id int) schema.Record {
	return schema.Record{
		"id":    int32(id),
		"name":  fmt.Sprintf("record_%d", id),
		"value": float64(id) * 3.14,
	}
}

// ====================
// INSERT Benchmarks
// ====================

func BenchmarkBTreeInsert_100(b *testing.B) {
	benchBTreeInsert(b, 100)
}

func BenchmarkBTreeInsert_1000(b *testing.B) {
	benchBTreeInsert(b, 1000)
}

func BenchmarkBTreeInsert_10000(b *testing.B) {
	benchBTreeInsert(b, 10000)
}

func BenchmarkTableStoreInsert_100(b *testing.B) {
	benchTableStoreInsert(b, 100)
}

func BenchmarkTableStoreInsert_1000(b *testing.B) {
	benchTableStoreInsert(b, 1000)
}

func BenchmarkTableStoreInsert_10000(b *testing.B) {
	benchTableStoreInsert(b, 10000)
}

func benchBTreeInsert(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filename := fmt.Sprintf("/tmp/bench_btree_insert_%d.db", i)
		defer os.Remove(filename)

		store, err := CreateBTreeStore(filename, benchSchema())
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()

		b.StartTimer()
		for j := 0; j < n; j++ {
			if err := store.Insert(benchRecord(j)); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}

func benchTableStoreInsert(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filename := fmt.Sprintf("/tmp/bench_table_insert_%d.db", i)
		defer os.Remove(filename)

		store, err := CreateTableStore(filename, benchSchema())
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()

		b.StartTimer()
		for j := 0; j < n; j++ {
			if err := store.Insert(benchRecord(j)); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}

// ====================
// FIND Benchmarks
// ====================

func BenchmarkBTreeFind_100(b *testing.B) {
	benchBTreeFind(b, 100)
}

func BenchmarkBTreeFind_1000(b *testing.B) {
	benchBTreeFind(b, 1000)
}

func BenchmarkBTreeFind_10000(b *testing.B) {
	benchBTreeFind(b, 10000)
}

func BenchmarkTableStoreFind_100(b *testing.B) {
	benchTableStoreFind(b, 100)
}

func BenchmarkTableStoreFind_1000(b *testing.B) {
	benchTableStoreFind(b, 1000)
}

func BenchmarkTableStoreFind_10000(b *testing.B) {
	benchTableStoreFind(b, 10000)
}

func benchBTreeFind(b *testing.B, n int) {
	filename := "/tmp/bench_btree_find.db"
	defer os.Remove(filename)

	store, err := CreateBTreeStore(filename, benchSchema())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Populate with n records
	for j := 0; j < n; j++ {
		if err := store.Insert(benchRecord(j)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Search for middle record
		_, err := store.Find(n / 2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchTableStoreFind(b *testing.B, n int) {
	filename := "/tmp/bench_table_find.db"
	defer os.Remove(filename)

	store, err := CreateTableStore(filename, benchSchema())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Populate with n records
	for j := 0; j < n; j++ {
		if err := store.Insert(benchRecord(j)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Search for middle record
		_, err := store.Find(n / 2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ====================
// SCAN Benchmarks
// ====================

func BenchmarkBTreeScanAll_100(b *testing.B) {
	benchBTreeScanAll(b, 100)
}

func BenchmarkBTreeScanAll_1000(b *testing.B) {
	benchBTreeScanAll(b, 1000)
}

func BenchmarkBTreeScanAll_10000(b *testing.B) {
	benchBTreeScanAll(b, 10000)
}

func BenchmarkTableStoreScanAll_100(b *testing.B) {
	benchTableStoreScanAll(b, 100)
}

func BenchmarkTableStoreScanAll_1000(b *testing.B) {
	benchTableStoreScanAll(b, 1000)
}

func BenchmarkTableStoreScanAll_10000(b *testing.B) {
	benchTableStoreScanAll(b, 10000)
}

func benchBTreeScanAll(b *testing.B, n int) {
	filename := "/tmp/bench_btree_scan.db"
	defer os.Remove(filename)

	store, err := CreateBTreeStore(filename, benchSchema())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Populate with n records
	for j := 0; j < n; j++ {
		if err := store.Insert(benchRecord(j)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.ScanAll()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchTableStoreScanAll(b *testing.B, n int) {
	filename := "/tmp/bench_table_scan.db"
	defer os.Remove(filename)

	store, err := CreateTableStore(filename, benchSchema())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Populate with n records
	for j := 0; j < n; j++ {
		if err := store.Insert(benchRecord(j)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.ScanAll()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ====================
// DELETE Benchmarks (BTree only)
// ====================

func BenchmarkBTreeDelete_100(b *testing.B) {
	benchBTreeDelete(b, 100)
}

func BenchmarkBTreeDelete_1000(b *testing.B) {
	benchBTreeDelete(b, 1000)
}

func benchBTreeDelete(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filename := fmt.Sprintf("/tmp/bench_btree_delete_%d.db", i)
		defer os.Remove(filename)

		store, err := CreateBTreeStore(filename, benchSchema())
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()

		// Populate with n records
		for j := 0; j < n; j++ {
			if err := store.Insert(benchRecord(j)); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		// Delete every other record
		for j := 0; j < n; j += 2 {
			if err := store.Delete(uint64(j)); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}

// ====================
// Mixed Workload Benchmarks
// ====================

func BenchmarkBTreeMixed_1000(b *testing.B) {
	benchBTreeMixed(b, 1000)
}

func BenchmarkTableStoreMixed_1000(b *testing.B) {
	benchTableStoreMixed(b, 1000)
}

func benchBTreeMixed(b *testing.B, n int) {
	filename := "/tmp/bench_btree_mixed.db"
	defer os.Remove(filename)

	store, err := CreateBTreeStore(filename, benchSchema())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Insert
		if err := store.Insert(benchRecord(i)); err != nil {
			b.Fatal(err)
		}
		// Find
		if i > 0 {
			_, err := store.Find(i - 1)
			if err != nil {
				b.Fatal(err)
			}
		}
		// Delete every 10th
		if i%10 == 0 && i > 0 {
			store.Delete(uint64(i - 10))
		}
	}
}

func benchTableStoreMixed(b *testing.B, n int) {
	filename := "/tmp/bench_table_mixed.db"
	defer os.Remove(filename)

	store, err := CreateTableStore(filename, benchSchema())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Insert
		if err := store.Insert(benchRecord(i)); err != nil {
			b.Fatal(err)
		}
		// Find
		if i > 0 {
			_, err := store.Find(i - 1)
			if err != nil {
				b.Fatal(err)
			}
		}
		// TableStore doesn't support delete
	}
}
