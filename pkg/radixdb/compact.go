package radixdb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const (
	compactTmpPattern = ".rdx2-compact-*.tmp"
	compactBakSuffix  = ".compact.bak"
)

// CompactFile copies the live tree from srcPath into dstPath using a fresh bump allocator.
// The destination is written atomically: data is written to a temporary file in the
// same directory as dstPath, then renamed to dstPath. If the destination already exists,
// it is first renamed to dstPath.compact.bak and removed after success; on failure the
// backup is restored when possible.
// If srcPath and dstPath refer to the same file, the source database is closed before
// the swap so the file can be replaced in place.
func CompactFile(srcPath, dstPath string) error {
	if srcPath == "" || dstPath == "" {
		return errors.New("radixdb: empty path")
	}
	srcPath = filepath.Clean(srcPath)
	dstPath = filepath.Clean(dstPath)

	dir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, compactTmpPattern)
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()

	src, err := OpenReadOnly(srcPath)
	if err != nil {
		return err
	}

	dst, err := Open(tmpName)
	if err != nil {
		_ = src.Close()
		return err
	}

	var insertErr error
	if err := src.WalkPrefixBytes(nil, func(k []byte, rows []Row) bool {
		key := string(append([]byte(nil), k...))
		for _, r := range rows {
			rr := Row{
				ParentID: r.ParentID,
				ID:       r.ID,
				FullPath: strings.Clone(r.FullPath),
			}
			if err := dst.Insert(key, rr); err != nil {
				insertErr = err
				return true
			}
		}
		return false
	}); err != nil {
		_ = dst.Close()
		_ = src.Close()
		return err
	}
	if insertErr != nil {
		_ = dst.Close()
		_ = src.Close()
		return insertErr
	}

	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		_ = src.Close()
		return err
	}
	if err := dst.Close(); err != nil {
		_ = src.Close()
		return err
	}
	if err := src.Close(); err != nil {
		return err
	}

	inPlace, err := sameAbsPath(srcPath, dstPath)
	if err != nil {
		return err
	}
	if err := atomicReplaceWithBackup(dstPath, tmpName, inPlace); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

func atomicReplaceWithBackup(dstPath, tmpName string, inPlace bool) error {
	bakPath := dstPath + compactBakSuffix
	_ = os.Remove(bakPath)

	_, statErr := os.Stat(dstPath)
	if statErr != nil && !os.IsNotExist(statErr) {
		return statErr
	}
	dstExists := statErr == nil

	if dstExists || inPlace {
		if err := os.Rename(dstPath, bakPath); err != nil {
			if os.IsNotExist(err) && !inPlace {
				return os.Rename(tmpName, dstPath)
			}
			return err
		}
		if err := os.Rename(tmpName, dstPath); err != nil {
			_ = os.Rename(bakPath, dstPath)
			return err
		}
		_ = os.Remove(bakPath)
		return nil
	}

	return os.Rename(tmpName, dstPath)
}

func sameAbsPath(a, b string) (bool, error) {
	aa, err := filepath.Abs(a)
	if err != nil {
		return false, err
	}
	bb, err := filepath.Abs(b)
	if err != nil {
		return false, err
	}
	return aa == bb, nil
}
