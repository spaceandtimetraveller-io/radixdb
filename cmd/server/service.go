package main

import (
	"context"
	"errors"
	"math"
	"sync"

	pb "artbenchmark/proto/radixdb/v1"
	"artbenchmark/radixdb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type radixService struct {
	pb.UnimplementedRadixDBServer
	db *radixdb.DB

	mu          sync.Mutex
	parentIndex []string
	nextID      int64 // next auto id; > MaxInt32 means no auto ids left
}

func growParentIndex(pi *[]string, id int) {
	if id < 0 {
		return
	}
	if len(*pi) <= id {
		grow := make([]string, id+1-len(*pi))
		*pi = append(*pi, grow...)
	}
}

// hydrateParentIndex rebuilds id → full_path from persisted rows and nextID = max(id)+1.
func (s *radixService) hydrateParentIndex() error {
	var maxID int32
	err := s.db.WalkPrefixBytes(nil, func(_ []byte, rows []radixdb.Row) bool {
		for _, r := range rows {
			if r.ID <= 0 {
				continue
			}
			if r.ID > maxID {
				maxID = r.ID
			}
			growParentIndex(&s.parentIndex, int(r.ID))
			s.parentIndex[r.ID] = r.FullPath
		}
		return false
	})
	if err != nil {
		return err
	}
	s.nextID = int64(maxID) + 1
	if s.nextID < 1 {
		s.nextID = 1
	}
	return nil
}

func rowToProto(r radixdb.Row) *pb.Row {
	return &pb.Row{
		ParentId: r.ParentID,
		Id:       r.ID,
		FullPath: r.FullPath,
	}
}

func (s *radixService) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	if req == nil || req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key required")
	}

	parentID := req.GetParentId()
	if parentID < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid parent_id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var id int32
	if req.Id != nil {
		id = *req.Id
		if id <= 0 || parentID >= id {
			return nil, status.Error(codes.InvalidArgument, "invalid parent_id and id (need id > 0 and parent_id < id)")
		}
	} else {
		if s.nextID > math.MaxInt32 {
			return nil, status.Error(codes.ResourceExhausted, "id space exhausted")
		}
		id = int32(s.nextID)
		if parentID >= id {
			return nil, status.Error(codes.InvalidArgument, "invalid parent_id for auto id (need parent_id < assigned id)")
		}
	}

	var fullPath string
	if parentID == 0 {
		fullPath = req.Key
	} else {
		growParentIndex(&s.parentIndex, int(parentID))
		fp := s.parentIndex[parentID]
		fullPath = fp + ">" + req.Key
	}

	_, keyExisted, err := s.db.Get(req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	row := radixdb.Row{ParentID: parentID, ID: id, FullPath: fullPath}
	if err := s.db.Insert(req.Key, row); err != nil {
		if errors.Is(err, radixdb.ErrReadOnly) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	bumpInsertMetrics(keyExisted)

	if req.Id == nil {
		s.nextID++
	}

	growParentIndex(&s.parentIndex, int(id))
	s.parentIndex[id] = fullPath

	return &pb.InsertResponse{Id: id}, nil
}

func (s *radixService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	rows, found, err := s.db.Get(req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &pb.GetResponse{Found: found}
	if !found {
		return out, nil
	}
	out.Rows = make([]*pb.Row, len(rows))
	for i := range rows {
		out.Rows[i] = rowToProto(rows[i])
	}
	return out, nil
}

func (s *radixService) WalkPrefix(req *pb.WalkPrefixRequest, stream grpc.ServerStreamingServer[pb.KeyRows]) error {
	prefix := ""
	if req != nil {
		prefix = req.Prefix
	}
	var sendErr error
	err := s.db.WalkPrefixBytes([]byte(prefix), func(key []byte, rows []radixdb.Row) bool {
		kr := &pb.KeyRows{
			Key:  string(key),
			Rows: make([]*pb.Row, len(rows)),
		}
		for i := range rows {
			kr.Rows[i] = rowToProto(rows[i])
		}
		if err := stream.Send(kr); err != nil {
			sendErr = err
			return true
		}
		return false
	})
	if sendErr != nil {
		return sendErr
	}
	return err
}

func (s *radixService) Sync(ctx context.Context, _ *pb.SyncRequest) (*pb.SyncResponse, error) {
	if err := s.db.Sync(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SyncResponse{}, nil
}

func (s *radixService) Stats(ctx context.Context, _ *pb.StatsRequest) (*pb.StatsResponse, error) {
	distinct, total, err := s.db.Stats()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.StatsResponse{DistinctKeys: distinct, TotalRows: total}, nil
}
