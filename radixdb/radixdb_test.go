package radixdb

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

func TestInsertGetMultiValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "MERKEZ"
	if err := db.Insert(key, Row{ParentID: 1, ID: 10, FullPath: "a>b"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert(key, Row{ParentID: 2, ID: 20, FullPath: "c>d"}); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(rows) != 2 {
		t.Fatalf("got ok=%v rows=%d", ok, len(rows))
	}
	if rows[0].ID != 10 || rows[1].ID != 20 {
		t.Fatalf("rows: %+v", rows)
	}
}

func TestWalkPrefixVsIRadix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type agg struct{ rows []Row }
	tree := iradix.New[*agg]()
	keys := []string{"ABDİX", "ABDİY", "MERKEZ", "ABC"}
	for _, k := range keys {
		r := Row{ParentID: 0, ID: 1, FullPath: k}
		if err := db.Insert(k, r); err != nil {
			t.Fatal(err)
		}
		txn := tree.Txn()
		var a *agg
		if old, ok := txn.Get([]byte(k)); ok {
			a = old
		} else {
			a = &agg{}
		}
		a.rows = append(a.rows, r)
		txn.Insert([]byte(k), a)
		tree = txn.Commit()
	}

	prefix := "ABDİ"
	var mmapKeys []string
	_ = db.WalkPrefix(prefix, func(key string, rows []Row) bool {
		mmapKeys = append(mmapKeys, key)
		return false
	})
	var irKeys []string
	tree.Root().WalkPrefix([]byte(prefix), func(k []byte, v *agg) bool {
		irKeys = append(irKeys, string(k))
		return false
	})
	sort.Strings(mmapKeys)
	sort.Strings(irKeys)
	if len(mmapKeys) != len(irKeys) {
		t.Fatalf("count mmap=%d iradix=%d mmap=%v ir=%v", len(mmapKeys), len(irKeys), mmapKeys, irKeys)
	}
	for i := range mmapKeys {
		if mmapKeys[i] != irKeys[i] {
			t.Fatalf("key mismatch %q vs %q", mmapKeys[i], irKeys[i])
		}
	}
}

func TestLockFreeReadRoot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("k", Row{}); err != nil {
		t.Fatal(err)
	}
	// concurrent reads without writer lock
	done := make(chan bool, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, _, _ = db.Get("k")
				_ = db.WalkPrefix("", func(string, []Row) bool { return false })
			}
			done <- true
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}

func TestNeighborhoodCSVLoad(t *testing.T) {
	csvPath := filepath.Join("..", "benchs", "neigborhood.csv")
	if _, err := os.Stat(csvPath); err != nil {
		t.Skip("neigborhood.csv not in benchs/")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "big.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadNeighborhoodCSV(csvPath, db); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = db.WalkPrefix("ABDİ", func(string, []Row) bool {
		n++
		return n >= 50
	})
	if n == 0 {
		t.Fatal("expected some prefix matches")
	}
}

func TestStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("k", Row{ParentID: 0, ID: 2, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("j", Row{ParentID: 0, ID: 3, FullPath: "j"}); err != nil {
		t.Fatal(err)
	}
	dk, tr, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if dk != 2 || tr != 3 {
		t.Fatalf("Stats: distinct=%d total=%d", dk, tr)
	}
}

func TestStatsReopenReadsHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats_reopen.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("k", Row{ParentID: 0, ID: 2, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("j", Row{ParentID: 0, ID: 3, FullPath: "j"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	dk, tr, err := db2.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if dk != 2 || tr != 3 {
		t.Fatalf("Stats after reopen: distinct=%d total=%d", dk, tr)
	}
}

func TestStatsLegacyHeaderMigrationRW(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats_legacy.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("k", Row{ParentID: 0, ID: 2, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("j", Row{ParentID: 0, ID: 3, FullPath: "j"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 16; i < minHeader; i++ {
		buf[i] = 0
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	dk, tr, err := db2.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if dk != 2 || tr != 3 {
		t.Fatalf("Stats after legacy migration: distinct=%d total=%d", dk, tr)
	}
	if db2.mmap[hdrStatsValid] != 1 {
		t.Fatal("expected statsValid byte after RW migration")
	}
}

func TestStatsReadOnlyLegacyWalkOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats_ro.rdx")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("x", Row{ParentID: 0, ID: 1, FullPath: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 16; i < minHeader; i++ {
		buf[i] = 0
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	dk, tr, err := ro.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if dk != 1 || tr != 1 {
		t.Fatalf("read-only legacy Stats: distinct=%d total=%d", dk, tr)
	}
	dk2, tr2, err := ro.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if dk2 != dk || tr2 != tr {
		t.Fatalf("second Stats mismatch")
	}
}
