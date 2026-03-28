package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"artbenchmark/pkg/radixdb"
	pb "artbenchmark/proto/radixdb/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestInsertComputesFullPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.rdx2")
	db, err := radixdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := &radixService{db: db}
	if err := svc.hydrateParentIndex(); err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterRadixDBServer(s, svc)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { s.Stop() })

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := pb.NewRadixDBClient(conn)

	id1 := int32(1)
	id2 := int32(2)
	if _, err := cli.Insert(ctx, &pb.InsertRequest{Key: "A", Id: &id1}); err != nil {
		t.Fatalf("insert root: %v", err)
	}
	p := int32(1)
	if _, err := cli.Insert(ctx, &pb.InsertRequest{Key: "B", Id: &id2, ParentId: &p}); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	g, err := cli.Get(ctx, &pb.GetRequest{Key: "B"})
	if err != nil {
		t.Fatal(err)
	}
	if !g.Found || len(g.Rows) != 1 {
		t.Fatalf("Get B: found=%v rows=%d", g.Found, len(g.Rows))
	}
	if got := g.Rows[0].FullPath; got != "A>B" {
		t.Fatalf("full_path: got %q want %q", got, "A>B")
	}
}

func TestHydrateParentIndexAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.rdx2")

	db1, err := radixdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc1 := &radixService{db: db1}
	_ = svc1.hydrateParentIndex()
	id1 := int32(1)
	if _, err := svc1.Insert(context.Background(), &pb.InsertRequest{Key: "A", Id: &id1}); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = db1.Close()

	db2, err := radixdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	svc2 := &radixService{db: db2}
	if err := svc2.hydrateParentIndex(); err != nil {
		t.Fatal(err)
	}
	p := int32(1)
	id2 := int32(2)
	if _, err := svc2.Insert(context.Background(), &pb.InsertRequest{Key: "B", Id: &id2, ParentId: &p}); err != nil {
		t.Fatalf("after hydrate, child insert: %v", err)
	}
	rows, found, err := db2.Get("B")
	if err != nil || !found || len(rows) != 1 || rows[0].FullPath != "A>B" {
		t.Fatalf("Get B: found=%v rows=%v err=%v", found, rows, err)
	}
}

func TestInsertAutoincrementID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.rdx2")
	db, err := radixdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := &radixService{db: db}
	if err := svc.hydrateParentIndex(); err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterRadixDBServer(s, svc)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { s.Stop() })

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := pb.NewRadixDBClient(conn)

	r1, err := cli.Insert(ctx, &pb.InsertRequest{Key: "A"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.GetId() != 1 {
		t.Fatalf("first auto id: got %d want 1", r1.GetId())
	}
	p1 := int32(1)
	r2, err := cli.Insert(ctx, &pb.InsertRequest{Key: "B", ParentId: &p1})
	if err != nil {
		t.Fatal(err)
	}
	if r2.GetId() != 2 {
		t.Fatalf("second auto id: got %d want 2", r2.GetId())
	}
}

func TestStatsRPC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.rdx2")
	db, err := radixdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := &radixService{db: db}
	if err := svc.hydrateParentIndex(); err != nil {
		t.Fatal(err)
	}
	a, b := int32(1), int32(2)
	if _, err := svc.Insert(context.Background(), &pb.InsertRequest{Key: "K", Id: &a}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Insert(context.Background(), &pb.InsertRequest{Key: "K", Id: &b}); err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterRadixDBServer(s, svc)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { s.Stop() })

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := pb.NewRadixDBClient(conn)

	st, err := cli.Stats(ctx, &pb.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.GetDistinctKeys() != 1 || st.GetTotalRows() != 2 {
		t.Fatalf("stats: distinct=%d total=%d", st.GetDistinctKeys(), st.GetTotalRows())
	}
}

func TestWalkPrefixLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "walklim.rdx2")
	db, err := radixdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := &radixService{db: db}
	if err := svc.hydrateParentIndex(); err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterRadixDBServer(s, svc)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { s.Stop() })

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := pb.NewRadixDBClient(conn)

	for _, k := range []string{"apple", "apricot", "banana"} {
		if _, err := cli.Insert(ctx, &pb.InsertRequest{Key: k}); err != nil {
			t.Fatalf("insert %q: %v", k, err)
		}
	}

	st, err := cli.WalkPrefix(ctx, &pb.WalkPrefixRequest{Prefix: "ap", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		_, err := st.Recv()
		if err != nil {
			break
		}
		n++
	}
	if n != 1 {
		t.Fatalf("limit=1: got %d messages want 1", n)
	}

	st2, err := cli.WalkPrefix(ctx, &pb.WalkPrefixRequest{Prefix: "", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	n = 0
	for {
		_, err := st2.Recv()
		if err != nil {
			break
		}
		n++
	}
	if n != 2 {
		t.Fatalf("limit=2 full walk: got %d messages want 2", n)
	}
}
