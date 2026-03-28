// CLI talks to radixdb gRPC (cmd/server). Example:
//
//	go run ./cmd/cli -addr 127.0.0.1:50051 -file benchs/neigborhood.csv
//	go run ./cmd/cli -key MERKEZ -id 1 -parent_id 0
//	go run ./cmd/cli -key NEWNODE -parent_id 0   # omit -id for server autoincrement
//	go run ./cmd/cli -prefix ABDİ
//	go run ./cmd/cli -stats
//	go run ./cmd/cli -addr :50052 -file benchs/neigborhood.csv -bench
//	go run ./cmd/cli -addr :50052 -file benchs/neigborhood.csv -bench -bench-skip-load   # server already has data
//	go run ./cmd/cli -addr :50052 -file benchs/neigborhood.csv -bench -c 8 -bench-skip-load
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "artbenchmark/proto/radixdb/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address (host:port)")
	file := flag.String("file", "", "CSV path: semicolon-separated id;name;parent_id (bulk insert)")
	key := flag.String("key", "", "key for single insert (requires -file to be empty)")
	id := flag.Int("id", -1, "numeric id for single insert (omit for server autoincrement)")
	parentID := flag.Int("parent_id", 0, "parent id for single insert (0 = root)")
	prefix := flag.String("prefix", "", "walk keys with this byte prefix and print rows")
	syncAfter := flag.Bool("sync", false, "call Sync after inserts")
	showStats := flag.Bool("stats", false, "print distinct_keys and total_rows")
	bench := flag.Bool("bench", false, "after -file: WalkPrefix benchmark (sample keys from CSV, random strict prefixes)")
	benchKeys := flag.Int("bench-keys", 1000, "number of keys to sample for -bench (max: unique keys in CSV)")
	benchSkipLoad := flag.Bool("bench-skip-load", false, "with -bench -file: do not Insert; only read CSV for key names (DB must already contain them)")
	benchConcurrency := flag.Int("c", 1, "for -bench: concurrent goroutines issuing WalkPrefix (default 1)")
	walkLimit := flag.Uint("walk-limit", 0, "max distinct keys per WalkPrefix for -prefix and -bench (0 = unlimited)")
	flag.Parse()

	if *file != "" && (*key != "" || *id != -1) {
		log.Fatal("use either -file or single -key/-id, not both")
	}
	if *file == "" && *key != "" && *id == 0 {
		log.Fatal("-id must be omitted (autoincrement) or a positive value")
	}
	if *bench && *file == "" {
		log.Fatal("-bench requires -file <csv> to sample keys")
	}
	if *file == "" && *key == "" && *prefix == "" && !*showStats && !*bench {
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewRadixDBClient(conn)

	if *file != "" && !(*bench && *benchSkipLoad) {
		if err := loadCSV(ctx, client, *file); err != nil {
			log.Fatal(err)
		}
		fmt.Fprintln(os.Stderr, "loaded", *file)
	}

	if *file == "" && *key != "" {
		var idPtr *int32
		if *id > 0 {
			v := int32(*id)
			idPtr = &v
		}
		resp, err := insertOne(ctx, client, *key, idPtr, int32(*parentID))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(os.Stderr, "inserted key=%q id=%d parent_id=%d\n", *key, resp.GetId(), *parentID)
	}

	if *prefix != "" {
		if err := walkPrefix(ctx, client, *prefix, uint32(*walkLimit)); err != nil {
			log.Fatal(err)
		}
	}

	if *showStats {
		st, err := client.Stats(ctx, &pb.StatsRequest{})
		if err != nil {
			log.Fatalf("stats: %v", err)
		}
		fmt.Printf("distinct_keys=%d total_rows=%d\n", st.GetDistinctKeys(), st.GetTotalRows())
	}

	if *syncAfter && (*file != "" || *key != "") {
		if _, err := client.Sync(ctx, &pb.SyncRequest{}); err != nil {
			log.Fatalf("sync: %v", err)
		}
		fmt.Fprintln(os.Stderr, "sync ok")
	}
	if *bench {
		if err := runWalkPrefixBench(ctx, client, *file, *benchKeys, *benchConcurrency, uint32(*walkLimit)); err != nil {
			log.Fatal(err)
		}
	}
}

func insertOne(ctx context.Context, client pb.RadixDBClient, key string, id *int32, parentID int32) (*pb.InsertResponse, error) {
	req := &pb.InsertRequest{Key: key}
	if id != nil {
		req.Id = id
	}
	if parentID != 0 {
		p := parentID
		req.ParentId = &p
	}
	return client.Insert(ctx, req)
}

func loadCSV(ctx context.Context, client pb.RadixDBClient, path string) error {
	fp, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fp.Close()
	r := csv.NewReader(fp)
	r.Comma = ';'
	start := time.Now()
	records := 0
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			return fmt.Errorf("parse id: %w", err)
		}
		parentID, err := strconv.Atoi(record[2])
		if err != nil {
			return fmt.Errorf("parse parent_id: %w", err)
		}
		key := record[1]
		pid := int32(parentID)
		id32 := int32(id)
		if pid >= id32 {
			continue
		}
		if _, err := insertOne(ctx, client, key, &id32, pid); err != nil {
			return fmt.Errorf("insert %q: %w", key, err)
		}
		records++
	}
	fmt.Fprintf(os.Stderr, "inserted %d rows in %s\n", records, time.Since(start))
	return nil
}

func walkPrefix(ctx context.Context, client pb.RadixDBClient, pfx string, limit uint32) error {
	_, err := walkPrefixDrain(ctx, client, pfx, true, limit)
	return err
}

// walkPrefixDrain receives the full WalkPrefix stream; if printRows, prints like walkPrefix.
func walkPrefixDrain(ctx context.Context, client pb.RadixDBClient, pfx string, printRows bool, limit uint32) (messages int, err error) {
	req := &pb.WalkPrefixRequest{Prefix: pfx}
	if limit > 0 {
		req.Limit = limit
	}
	stream, err := client.WalkPrefix(ctx, req)
	if err != nil {
		return 0, err
	}
	for {
		kr, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		messages++
		if printRows {
			fmt.Println(kr.Key)
			for _, row := range kr.Rows {
				fmt.Printf("  parent_id=%d id=%d full_path=%q\n", row.ParentId, row.Id, row.FullPath)
			}
		}
	}
	return messages, nil
}

// collectKeysFromCSV returns insert keys using the same row filter as loadCSV (semicolon CSV).
func collectKeysFromCSV(path string) ([]string, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	r := csv.NewReader(fp)
	r.Comma = ';'
	var keys []string
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(record) < 3 {
			continue
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, fmt.Errorf("parse id: %w", err)
		}
		parentID, err := strconv.Atoi(record[2])
		if err != nil {
			return nil, fmt.Errorf("parse parent_id: %w", err)
		}
		pid := int32(parentID)
		id32 := int32(id)
		if pid >= id32 {
			continue
		}
		keys = append(keys, record[1])
	}
	return keys, nil
}

// randomStrictPrefix returns a UTF-8 safe strict prefix of key (shorter than the full key when possible).
func randomStrictPrefix(key string) string {
	rn := []rune(key)
	if len(rn) <= 1 {
		return key
	}
	// prefix length in [1, len(rn)-1]
	n := 1 + rand.IntN(len(rn)-1)
	return string(rn[:n])
}

func runWalkPrefixBench(ctx context.Context, client pb.RadixDBClient, csvPath string, wantKeys int, concurrency int, walkLimit uint32) error {
	if concurrency < 1 {
		concurrency = 1
	}
	keys, err := collectKeysFromCSV(csvPath)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("no keys in %s (after filter)", csvPath)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	if wantKeys > 0 && len(keys) > wantKeys {
		keys = keys[:wantKeys]
	}
	fmt.Fprintf(os.Stderr, "bench: %d keys from %s, concurrency=%d, WalkPrefix with random strict UTF-8 prefix each\n",
		len(keys), csvPath, concurrency)

	jobs := make(chan string, len(keys))
	for _, k := range keys {
		jobs <- k
	}
	close(jobs)

	var totalMsgs atomic.Int64
	var errMu sync.Mutex
	var benchErr error
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range jobs {
				pfx := randomStrictPrefix(k)
				n, err := walkPrefixDrain(ctx, client, pfx, false, walkLimit)
				if err != nil {
					errMu.Lock()
					if benchErr == nil {
						benchErr = fmt.Errorf("WalkPrefix prefix=%q (from key=%q): %w", pfx, k, err)
					}
					errMu.Unlock()
					return
				}
				totalMsgs.Add(int64(n))
			}
		}()
	}
	wg.Wait()
	if benchErr != nil {
		return benchErr
	}
	d := time.Since(start)
	secs := d.Seconds()
	if secs < 1e-9 {
		secs = 1e-9
	}
	tm := totalMsgs.Load()
	fmt.Printf("walk_prefix_bench keys=%d concurrency=%d stream_messages=%d wall=%s (%.0f walks/s, %.0f msgs/s)\n",
		len(keys), concurrency, tm, d, float64(len(keys))/secs, float64(tm)/secs)
	return nil
}
