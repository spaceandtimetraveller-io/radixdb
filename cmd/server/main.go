// Server exposes artbenchmark/pkg/radixdb over gRPC (API from artbenchmark/proto/radixdb/v1).
//
// Regenerate protobuf Go stubs from repository root:
//
//	PATH="$PATH:$(go env GOPATH)/bin" protoc -I . --go_out=. --go_opt=paths=source_relative \
//	  --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/radixdb/v1/radixdb.proto
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"artbenchmark/pkg/radixdb"
	pb "artbenchmark/proto/radixdb/v1"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50052", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":9091", "HTTP listen address for Prometheus /metrics (empty to disable)")
	dbPath := flag.String("db", "radix2.rdx2", "path to RDX2 radixdb database file (.rdx2; created if missing)")
	readOnly := flag.Bool("readonly", false, "open database read-only (Insert/Sync will fail)")
	compactInterval := flag.Duration("compact-interval", 0, "if >0 and not readonly, run CompactIfNeeded on this interval (blocks readers while compacting)")
	flag.Parse()

	var db *radixdb.DB
	var err error
	if *readOnly {
		db, err = radixdb.OpenReadOnly(*dbPath)
	} else {
		db, err = radixdb.Open(*dbPath)
	}
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	svc := &radixService{db: db}
	if err := svc.hydrateParentIndex(); err != nil {
		log.Fatalf("hydrate parent index: %v", err)
	}
	if err := refreshMetricsFromDB(db); err != nil {
		log.Fatalf("metrics init: %v", err)
	}

	grpc_prometheus.EnableHandlingTimeHistogram()
	srv := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	pb.RegisterRadixDBServer(srv, svc)
	grpc_prometheus.Register(srv)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	var metricsServer *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metricsHandler())
		metricsServer = &http.Server{Addr: *metricsAddr, Handler: mux}
		go func() {
			log.Printf("prometheus metrics on http://%s/metrics", *metricsAddr)
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("metrics server: %v", err)
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *compactInterval > 0 && !*readOnly {
		go runPeriodicCompaction(ctx, db, *compactInterval)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if metricsServer != nil {
			_ = metricsServer.Shutdown(shutdownCtx)
		}
		srv.GracefulStop()
	}()

	log.Printf("radixdb gRPC listening on %s (db=%s)", *addr, *dbPath)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
