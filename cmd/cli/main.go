// CLI talks to radixdb gRPC (cmd/server). Example:
//
//	go run ./cmd/cli -addr 127.0.0.1:50051 -file benchs/neigborhood.csv
//	go run ./cmd/cli -key MERKEZ -id 1 -parent_id 0
//	go run ./cmd/cli -key NEWNODE -parent_id 0   # omit -id for server autoincrement
//	go run ./cmd/cli -prefix ABDİ
//	go run ./cmd/cli -stats
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
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
	flag.Parse()

	if *file != "" && (*key != "" || *id != -1) {
		log.Fatal("use either -file or single -key/-id, not both")
	}
	if *file == "" && *key != "" && *id == 0 {
		log.Fatal("-id must be omitted (autoincrement) or a positive value")
	}
	if *file == "" && *key == "" && *prefix == "" && !*showStats {
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

	if *file != "" {
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
		if err := walkPrefix(ctx, client, *prefix); err != nil {
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

func walkPrefix(ctx context.Context, client pb.RadixDBClient, pfx string) error {
	stream, err := client.WalkPrefix(ctx, &pb.WalkPrefixRequest{Prefix: pfx})
	if err != nil {
		return err
	}
	for {
		kr, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(kr.Key)
		for _, row := range kr.Rows {
			fmt.Printf("  parent_id=%d id=%d full_path=%q\n", row.ParentId, row.Id, row.FullPath)
		}
	}
	return nil
}
