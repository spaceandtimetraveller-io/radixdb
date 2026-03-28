// Compact performs offline compaction of an RDX2 radixdb database file (.rdx2): walks the live tree and
// writes a new file with a fresh bump allocator, then replaces the destination
// atomically. Use the same path for -src and -out to compact in place.
//
// Example:
//
//	go run ./cmd/compact -src data.rdx2 -out data.rdx2
//	go run ./cmd/compact -src big.rdx2 -out compact.rdx2
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"artbenchmark/pkg/radixdb"
)

func main() {
	src := flag.String("src", "", "source RDX2 database file (.rdx2)")
	out := flag.String("out", "", "output path (same as -src for in-place compaction)")
	flag.Parse()
	if *src == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: compact -src <path> -out <path>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if err := radixdb.CompactFile(*src, *out); err != nil {
		log.Fatal(err)
	}
}
