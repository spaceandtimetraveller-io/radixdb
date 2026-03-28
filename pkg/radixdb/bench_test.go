package radixdb

import (
	"path/filepath"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

// openBenchNeighborhoodDB opens a fresh DB in b.TempDir(), loads benchs/neigborhood.csv,
// and returns it for benchmarks. The caller must Close the DB. Skips the test if the CSV is missing.
func openBenchNeighborhoodDB(b *testing.B) *DB {
	b.Helper()
	csvPath := filepath.Join("..", "benchs", "neigborhood.csv")
	dir := b.TempDir()
	path := filepath.Join(dir, "neighborhood.rdx2")
	db, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := LoadNeighborhoodCSV(csvPath, db); err != nil {
		_ = db.Close()
		b.Skip(err)
	}
	return db
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "get.rdx2")
	db, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("lookup", Row{ParentID: 0, ID: 1, FullPath: "lookup"}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = db.Get("lookup")
	}
}

func BenchmarkInsert(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "ins.rdx2")
	db, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "a"}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Insert("k", Row{ParentID: 0, ID: int32(2 + i), FullPath: "b"})
	}
}

func BenchmarkWalkPrefixRadixdb2VsIRadix(b *testing.B) {
	db := openBenchNeighborhoodDB(b)
	defer db.Close()

	tree := iradix.New[*struct{}]()

	_ = db.WalkPrefix("", func(key string, rows []Row) bool {
		tree, _, _ = tree.Insert([]byte(key), &struct{}{})
		return false
	})

	prefix := "ABDİ"
	b.Run("radixdb", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			_ = db.WalkPrefix(prefix, func(string, []Row) bool {
				n++
				return n >= 100
			})
		}
	})
	b.Run("radixdb_bytes", func(b *testing.B) {
		p := []byte(prefix)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			_ = db.WalkPrefixBytes(p, func([]byte, []Row) bool {
				n++
				return n >= 100
			})
		}
	})
	b.Run("iradix", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			found := make([][]byte, 0, 100)
			tree.Root().WalkPrefix([]byte(prefix), func(k []byte, _ *struct{}) bool {
				found = append(found, k)
				return len(found) >= cap(found)
			})
		}
	})
}

// BenchmarkWalkPrefixOnly measures only WalkPrefixBytes after DB setup (good for pprof: use long -benchtime).
func BenchmarkWalkPrefixOnly(b *testing.B) {
	db := openBenchNeighborhoodDB(b)
	defer db.Close()

	prefix := []byte("ABDİ")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		_ = db.WalkPrefixBytes(prefix, func([]byte, []Row) bool {
			n++
			return n >= 100
		})
	}
}

// BenchmarkWalkPrefixParallel runs WalkPrefixBytes concurrently from multiple goroutines (GOMAXPROCS).
// Throughput is total walks across all workers; compare wall time vs BenchmarkWalkPrefixOnly.
func BenchmarkWalkPrefixParallel(b *testing.B) {
	db := openBenchNeighborhoodDB(b)
	defer db.Close()

	prefix := []byte("ABDİ")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := 0
			_ = db.WalkPrefixBytes(prefix, func([]byte, []Row) bool {
				n++
				return n >= 100
			})
		}
	})
}
