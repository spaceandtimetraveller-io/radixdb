package radixdb

import (
	"encoding/csv"
	"os"
	"strconv"
)

// LoadNeighborhoodCSV loads semicolon-separated rows id;name;parent_id like benchs/neigborhood.csv
// and applies the same merge semantics as benchs/benchmark_art_test.go Indexes.Insert.
func LoadNeighborhoodCSV(path string, db *DB) error {
	fp, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fp.Close()
	r := csv.NewReader(fp)
	r.Comma = ';'

	var parentIndex []string

	for {
		record, err := r.Read()
		if err != nil {
			break
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			return err
		}
		parentID, err := strconv.Atoi(record[2])
		if err != nil {
			return err
		}
		key := record[1]
		pid := int32(parentID)
		id32 := int32(id)
		if pid >= id32 {
			continue
		}
		if len(parentIndex) <= int(id32) {
			grow := make([]string, int(id32)+1-len(parentIndex))
			parentIndex = append(parentIndex, grow...)
		}
		if pid == 0 {
			parentIndex[int(id32)] = key
		}
		if pid > 0 {
			fp := parentIndex[int(pid)]
			fpKey := fp + ">" + key
			parentIndex[int(id32)] = fpKey
		}
		row := Row{
			ParentID: pid,
			ID:       id32,
			FullPath: parentIndex[int(id32)],
		}
		if err := db.Insert(key, row); err != nil {
			return err
		}
	}
	return nil
}
