package radixdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestInsertGetMultiValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.rdx2")
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

func TestReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "re.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "k"}); err != nil {
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
	rows, ok, err := db2.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(rows) != 1 {
		t.Fatalf("reopen get: ok=%v n=%d", ok, len(rows))
	}
}

func TestWalkPrefixStopAcrossSiblings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "walkstop.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, k := range []string{"apple", "apricot", "banana"} {
		if err := db.Insert(k, Row{ParentID: 0, ID: 1, FullPath: k}); err != nil {
			t.Fatalf("insert %q: %v", k, err)
		}
	}
	var n int
	err = db.WalkPrefixBytes([]byte("ap"), func(key []byte, _ []Row) bool {
		n++
		return n >= 1
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("callback stop after 1 key: visited %d keys", n)
	}
}

func TestConcurrentReadWhileWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "conc.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const seedKey = "seed"
	if err := db.Insert(seedKey, Row{ParentID: 0, ID: 1, FullPath: seedKey}); err != nil {
		t.Fatal(err)
	}

	const (
		numReaders = 8
		readIters  = 200
		numWrites  = 150
	)

	var wg sync.WaitGroup
	errCh := make(chan error, numReaders+1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range numWrites {
			key := fmt.Sprintf("w%d", i)
			r := Row{ParentID: 0, ID: int32(1000 + i), FullPath: key}
			if err := db.Insert(key, r); err != nil {
				errCh <- fmt.Errorf("insert %s: %w", key, err)
				return
			}
		}
		if err := db.Insert(seedKey, Row{ParentID: 0, ID: 2, FullPath: seedKey + "-dup"}); err != nil {
			errCh <- fmt.Errorf("dup seed: %w", err)
		}
	}()

	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range readIters {
				if _, _, err := db.Get(seedKey); err != nil {
					errCh <- fmt.Errorf("Get seed: %w", err)
					return
				}
				_, _, err := db.Get("w75")
				if err != nil {
					errCh <- fmt.Errorf("Get w75: %w", err)
					return
				}
				n := 0
				err = db.WalkPrefix("", func(string, []Row) bool {
					n++
					return n >= 15
				})
				if err != nil {
					errCh <- fmt.Errorf("WalkPrefix: %w", err)
					return
				}
				if _, _, err = db.Stats(); err != nil {
					errCh <- fmt.Errorf("Stats: %w", err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			t.Fatal(e)
		}
	}

	dk, tr, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	wantDistinct := uint64(numWrites + 1)
	wantTotal := uint64(2 + numWrites)
	if dk != wantDistinct || tr != wantTotal {
		t.Fatalf("Stats distinct=%d total=%d want %d %d", dk, tr, wantDistinct, wantTotal)
	}
}

func TestBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.rdx2")
	if err := os.WriteFile(path, make([]byte, BlockSize), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path)
	if err != ErrMagic {
		t.Fatalf("want ErrMagic got %v", err)
	}
}

func TestCompactFile(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.rdx2")
	outPath := filepath.Join(dir, "out.rdx2")

	db, err := Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"alpha", "beta", "alphabet"}
	for i, k := range keys {
		if err := db.Insert(k, Row{ParentID: 0, ID: int32(i + 1), FullPath: k}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Insert(keys[0], Row{ParentID: 1, ID: 99, FullPath: keys[0] + "-2"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := CompactFile(srcPath, outPath); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	d1, t1, err := openAndStats(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	d2, t2, err := db2.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 || t1 != t2 {
		t.Fatalf("stats src distinct=%d total=%d out distinct=%d total=%d", d1, t1, d2, t2)
	}

	for _, k := range keys {
		ra, oka, err := openAndGet(srcPath, k)
		if err != nil {
			t.Fatal(err)
		}
		rb, okb, err := db2.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if oka != okb || len(ra) != len(rb) {
			t.Fatalf("key %q len mismatch", k)
		}
		for i := range ra {
			if ra[i] != rb[i] {
				t.Fatalf("key %q row %d: %+v vs %+v", k, i, ra[i], rb[i])
			}
		}
	}

	inPlace := filepath.Join(dir, "inplace.rdx2")
	if err := copyFile(srcPath, inPlace); err != nil {
		t.Fatal(err)
	}
	if err := CompactFile(inPlace, inPlace); err != nil {
		t.Fatal(err)
	}
	d3, t3, err := openAndStats(inPlace)
	if err != nil {
		t.Fatal(err)
	}
	if d3 != d2 || t3 != t2 {
		t.Fatalf("in-place stats distinct=%d total=%d want %d %d", d3, t3, d2, t2)
	}
}

func openAndStats(path string) (uint64, uint64, error) {
	db, err := OpenReadOnly(path)
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	return db.Stats()
}

func openAndGet(path, key string) ([]Row, bool, error) {
	db, err := OpenReadOnly(path)
	if err != nil {
		return nil, false, err
	}
	defer db.Close()
	return db.Get(key)
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0o644)
}

func writeV1HeaderOnlyFile(t *testing.T, path string, root Ref, nBlocks, allocBlock, allocOff uint32, distinct, total uint64, statsValid bool) {
	t.Helper()
	buf := make([]byte, BlockSize)
	copy(buf[hdrMagicOff:hdrMagicOff+4], []byte("RDX2"))
	binary.LittleEndian.PutUint32(buf[hdrVersionOff:hdrVersionOff+4], Version1)
	binary.LittleEndian.PutUint64(buf[hdrRootRefOff:hdrRootRefOff+8], uint64(root))
	binary.LittleEndian.PutUint32(buf[hdrNBlocksOff:hdrNBlocksOff+4], nBlocks)
	binary.LittleEndian.PutUint32(buf[hdrAllocBlockOff:hdrAllocBlockOff+4], allocBlock)
	binary.LittleEndian.PutUint32(buf[hdrAllocOffOff:hdrAllocOffOff+4], allocOff)
	binary.LittleEndian.PutUint64(buf[hdrDistinctOff:hdrDistinctOff+8], distinct)
	binary.LittleEndian.PutUint64(buf[hdrTotalRowsOff:hdrTotalRowsOff+8], total)
	if statsValid {
		buf[hdrStatsValidOff] = 1
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestHeaderV1OpenAndMigratesOnFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v1.rdx2")
	writeV1HeaderOnlyFile(t, path, 0, 1, 1, 0, 0, 0, true)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	hdr, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(hdr[hdrVersionOff:hdrVersionOff+4]) != Version2 {
		t.Fatalf("expected header upgraded to version %d", Version2)
	}
}

func TestShouldCompactSmallFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ok, _, _, reason, err := db.ShouldCompact()
	if err != nil {
		t.Fatal(err)
	}
	if ok || reason != CompactSkipFileTooSmall {
		t.Fatalf("want file_too_small got ok=%v reason=%q", ok, reason)
	}
}

func TestLiveBytesNonEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("a", Row{ParentID: 0, ID: 1, FullPath: "a"}); err != nil {
		t.Fatal(err)
	}
	live, err := db.LiveBytes()
	if err != nil {
		t.Fatal(err)
	}
	if live == 0 || live > db.FileSizeBytes() {
		t.Fatalf("live=%d file=%d", live, db.FileSizeBytes())
	}
}

func TestCompactIfNeededSkipsWhenNoWaste(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nowaste.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("x", Row{ParentID: 0, ID: 1, FullPath: "x"}); err != nil {
		t.Fatal(err)
	}
	ran, reason, err := db.CompactIfNeeded()
	if err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatalf("unexpected compaction ran")
	}
	switch reason {
	case CompactSkipFileTooSmall, CompactSkipWasteBelowThresh, CompactSkipReclaimBelowMin, CompactSkipNoReclaimable:
	default:
		t.Fatalf("unexpected skip reason %q", reason)
	}
}

func TestCompactIfNeededSkipsNoWritesSinceLastCompact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idle_compact.rdx2")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Insert("k", Row{ParentID: 0, ID: 1, FullPath: "k"}); err != nil {
		t.Fatal(err)
	}
	// Pretend we already compacted at this write generation (no new inserts since).
	db.compactionLastRunUnixNano.Store(1)
	db.lastCompactWriteSeq.Store(db.writeSeq.Load())

	ran, reason, err := db.CompactIfNeeded()
	if err != nil {
		t.Fatal(err)
	}
	if ran || reason != CompactSkipNoWritesSinceCompact {
		t.Fatalf("want no_writes_since_compact, ran=%v reason=%q err=%v", ran, reason, err)
	}
}
