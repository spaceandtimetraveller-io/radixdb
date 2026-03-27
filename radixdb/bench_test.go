package radixdb

import (
	"path/filepath"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "get.rdx")
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
	path := filepath.Join(dir, "ins.rdx")
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

func BenchmarkWalkPrefixMmapVsIRadix(b *testing.B) {
	csvPath := filepath.Join("..", "benchs", "neigborhood.csv")
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.rdx")
	db, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := LoadNeighborhoodCSV(csvPath, db); err != nil {
		b.Skip(err)
	}

	tree := iradix.New[*struct{}]()

	// Build iradix with same keys only (single value per key for speed of setup)
	_ = db.WalkPrefix("", func(key string, rows []Row) bool {
		tree, _, _ = tree.Insert([]byte(key), &struct{}{})
		return false
	})

	prefix := "ABDİ"
	b.Run("mmapradix", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			_ = db.WalkPrefix(prefix, func(string, []Row) bool {
				n++
				return n >= 100
			})
		}
	})
	b.Run("mmapradix_bytes", func(b *testing.B) {
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
