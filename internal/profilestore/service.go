package profilestore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	profilestoregrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/profilestore/v1alpha1/profilestorev1alpha1grpc"
	profilestorepb "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/profilestore/v1alpha1"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/simonswine/parca2otlp/internal/arrowotlp"
	"github.com/simonswine/parca2otlp/internal/storage"
)

type Service struct {
	profilestoregrpc.UnimplementedProfileStoreServiceServer

	store *storage.Store
	mem   memory.Allocator
}

func New(store *storage.Store) *Service {
	return &Service{store: store, mem: memory.NewGoAllocator()}
}

func (s *Service) WriteArrow(ctx context.Context, request *profilestorepb.WriteArrowRequest) (*profilestorepb.WriteArrowResponse, error) {
	records, err := readRecords(request.GetIpcBuffer(), s.mem)
	if err != nil {
		return nil, invalidArgument(err)
	}
	defer release(records)

	export, err := arrowotlp.ConvertV2(records)
	if err != nil {
		return nil, invalidArgument(err)
	}
	if len(export.GetResourceProfiles()) == 0 {
		return &profilestorepb.WriteArrowResponse{}, nil
	}
	if _, err := s.store.Save(ctx, export); err != nil {
		return nil, status.Errorf(codes.Internal, "persist OTLP profiles: %v", err)
	}
	return &profilestorepb.WriteArrowResponse{}, nil
}

func (s *Service) Write(stream profilestoregrpc.ProfileStoreService_WriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		return invalidArgument(fmt.Errorf("receive sample record: %w", err))
	}
	sampleRecords, err := readRecords(first.GetRecord(), s.mem)
	if err != nil {
		return invalidArgument(err)
	}
	defer release(sampleRecords)
	if len(sampleRecords) != 1 {
		return invalidArgument(fmt.Errorf("expected one sample record, got %d", len(sampleRecords)))
	}

	ids, err := arrowotlp.StacktraceIDs(sampleRecords[0], s.mem)
	if err != nil {
		return invalidArgument(err)
	}
	if len(ids) > 0 {
		request, err := arrowotlp.StacktraceRequest(ids, s.mem)
		if err != nil {
			return status.Errorf(codes.Internal, "build stacktrace request: %v", err)
		}
		defer request.Release()
		data, err := writeRecord(request, s.mem)
		if err != nil {
			return status.Errorf(codes.Internal, "encode stacktrace request: %v", err)
		}
		if err := stream.Send(&profilestorepb.WriteResponse{Record: data}); err != nil {
			return status.Errorf(codes.Internal, "send stacktrace request: %v", err)
		}
	}

	var locations []arrowotlp.Record
	if len(ids) > 0 {
		second, err := stream.Recv()
		if err != nil {
			return invalidArgument(fmt.Errorf("receive locations record: %w", err))
		}
		locations, err = readRecords(second.GetRecord(), s.mem)
		if err != nil {
			return invalidArgument(err)
		}
		defer release(locations)
	}

	export, err := arrowotlp.ConvertV1(sampleRecords[0], locations)
	if err != nil {
		return invalidArgument(err)
	}
	if len(export.GetResourceProfiles()) > 0 {
		if _, err := s.store.Save(stream.Context(), export); err != nil {
			return status.Errorf(codes.Internal, "persist OTLP profiles: %v", err)
		}
	}
	return nil
}

func readRecords(data []byte, allocator memory.Allocator) ([]arrowotlp.Record, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(allocator))
	if err != nil {
		return nil, fmt.Errorf("open Arrow IPC payload: %w", err)
	}
	defer reader.Release()
	var records []arrowotlp.Record
	for reader.Next() {
		record := reader.Record()
		record.Retain()
		records = append(records, record)
	}
	if err := reader.Err(); err != nil {
		release(records)
		return nil, fmt.Errorf("read Arrow IPC payload: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("Arrow IPC payload contains no records")
	}
	return records, nil
}

func writeRecord(record arrowotlp.Record, allocator memory.Allocator) ([]byte, error) {
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(record.Schema()), ipc.WithAllocator(allocator))
	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func release(records []arrowotlp.Record) {
	for _, record := range records {
		record.Release()
	}
}

func invalidArgument(err error) error {
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "unexpected end of stream")
	}
	return status.Errorf(codes.InvalidArgument, "%v", err)
}
