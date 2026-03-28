# artbenchmark / radixdb

Block-paged, file-backed radix tree (**RDX2** on-disk format) with a gRPC API. The library lives in [`pkg/radixdb`](pkg/radixdb/), the API is defined in [`proto/radixdb/v1`](proto/radixdb/v1/), and this repo ships [`cmd/server`](cmd/server) (gRPC + Prometheus) and [`cmd/cli`](cmd/cli) (client).

## Architecture (`pkg/radixdb`)

- **On-disk layout**: One database file with magic `RDX2` and format version 1 or 2. The file is a sequence of fixed **4096-byte blocks**; block **0** holds the header (format version, root pointer, block count, allocator state, key/row stats, and v2 compaction tuning fields). Tree **nodes** store a byte prefix, sorted child edges, and an optional leaf reference; **leaves** hold row payloads. All in-tree pointers are **`Ref` values**: `uint64` with `blockId << 12 | offsetInBlock` (offset must fit in 12 bits), similar to swizzled pointers in DuckDB-style storage.

- **I/O model**: Persistence uses a normal `*os.File` with **`ReadAt` / `WriteAt`** and **`Truncate`** for growth. A small **`blockMgr`** keeps a per-block in-memory cache (`map[blockID][]byte`); there is **no `mmap`**. Callers must treat slices returned from reads as **read-only** when they alias cached buffers.

- **Inserts**: Descend the radix tree by key (UTF-8 bytes); updates are **copy-on-write** along the path. The **root `Ref`** is published only after all new blocks for that insert are written, so readers see a consistent tree.

- **Concurrency**: **`Insert`** and related mutations hold `DB.mu`. Concurrent readers coordinate with **`readCloseMu`** so **`Close`** can drain readers safely. **`OpenReadOnly`** opens the file read-only and rejects writes.

- **Compaction**: Dead space is estimated vs. reachable allocations (`LiveBytes`). **`CompactIfNeeded`** / **`CompactFile`** rewrite the live tree into a new layout when thresholds (size, reclaim, cooldown, waste ratio—configurable and persisted in the v2 header) are met. The server can run periodic compaction via **`-compact-interval`**.

## Prerequisites

- Go 1.22+ (see `go.mod`)

## Run the server

From the repository root:

```bash
go run ./cmd/server -addr :50052 -db /path/to/data.rdx2
```

Useful flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `:50052` | gRPC listen address (`host:port` or `:port`) |
| `-db` | `radix2.rdx2` | Path to the RDX2 database file (created if missing) |
| `-readonly` | `false` | Open the DB read-only (`Insert` / `Sync` fail) |
| `-metrics-addr` | `:9091` | Prometheus HTTP endpoint; set to empty string to disable |
| `-compact-interval` | `0` | If `>0` and not read-only, run `CompactIfNeeded` on this interval |

Examples:

```bash
# Default: gRPC on :50052, DB file ./radix2.rdx2, metrics on :9091
go run ./cmd/server

# Custom paths
go run ./cmd/server -addr 127.0.0.1:50052 -db ./mydata.rdx2 -metrics-addr :9091
```

Stop the server with `Ctrl+C` (graceful shutdown).

## CLI client (`cmd/cli`)

The CLI connects to the gRPC server (insecure). Default address is `127.0.0.1:50051`; **set `-addr` to match your server** (e.g. `127.0.0.1:50052` if you use the server defaults above).

```bash
go run ./cmd/cli -addr 127.0.0.1:50052 <flags>
```

You can combine flags in one invocation (e.g. load CSV, then print stats).

### Load data from CSV

CSV format: **semicolon-separated** columns `id;name;parent_id` (same idea as [`benchs/neigborhood.csv`](benchs/neigborhood.csv)).

```bash
go run ./cmd/cli -addr 127.0.0.1:50052 -file benchs/neigborhood.csv
```

Rows where `parent_id >= id` are skipped (invalid hierarchy). Each valid row is sent as an `Insert` with the given `key` (= name) and ids.

### Insert a single key

```bash
# Explicit id and parent (parent_id must be less than id when both set)
go run ./cmd/cli -addr 127.0.0.1:50052 -key MERKEZ -id 1 -parent_id 0

# Let the server assign the next id (omit -id)
go run ./cmd/cli -addr 127.0.0.1:50052 -key NEWNODE -parent_id 0
```

### Stats (distinct keys and total rows)

```bash
go run ./cmd/cli -addr 127.0.0.1:50052 -stats
```

Prints `distinct_keys` and `total_rows` (including multiple rows per key).

### Search: prefix walk (stream keys)

```bash
go run ./cmd/cli -addr 127.0.0.1:50052 -prefix ABDİ
```

Streams all keys whose UTF-8 byte sequence has the given prefix, printing each key and its rows (`parent_id`, `id`, `full_path`).

### Optional: sync after writes

```bash
go run ./cmd/cli -addr 127.0.0.1:50052 -file benchs/neigborhood.csv -sync
```

Calls `Sync` after bulk or single insert so the backing file is fsync’d.

### Get by exact key (gRPC)

The CLI does not expose `Get`. Use [`grpcurl`](https://github.com/fullstorydev/grpcurl) or another gRPC client, for example:

```bash
grpcurl -plaintext -d '{"key":"MERKEZ"}' 127.0.0.1:50052 radixdb.v1.RadixDB/Get
```

## Regenerating protobuf Go code

If you change [`proto/radixdb/v1/radixdb.proto`](proto/radixdb/v1/radixdb.proto), regenerate stubs from the repo root:

```bash
PATH="$PATH:$(go env GOPATH)/bin" protoc -I . --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/radixdb/v1/radixdb.proto
```

## Typical workflow

1. Start the server: `go run ./cmd/server -db ./data.rdx2`
2. Load data: `go run ./cmd/cli -addr 127.0.0.1:50052 -file benchs/neigborhood.csv`
3. Check stats: `go run ./cmd/cli -addr 127.0.0.1:50052 -stats`
4. Prefix search: `go run ./cmd/cli -addr 127.0.0.1:50052 -prefix SOME_PREFIX`
5. Insert more rows: `go run ./cmd/cli -addr 127.0.0.1:50052 -key NEW -parent_id 0` (or with `-id`)
