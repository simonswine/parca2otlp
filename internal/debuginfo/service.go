package debuginfo

import (
	"context"
	"errors"
	"fmt"
	"io"

	debuginfogrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/debuginfo/v1alpha1/debuginfov1alpha1grpc"
	debuginfopb "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/debuginfo/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	debuginfogrpc.UnimplementedDebuginfoServiceServer
	store *Store
}

func NewService(store *Store) *Service { return &Service{store: store} }

func (s *Service) ShouldInitiateUpload(ctx context.Context, request *debuginfopb.ShouldInitiateUploadRequest) (*debuginfopb.ShouldInitiateUploadResponse, error) {
	should, reason, err := s.store.ShouldUpload(request.GetBuildId(), request.GetType(), request.GetForce())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &debuginfopb.ShouldInitiateUploadResponse{ShouldInitiateUpload: should, Reason: reason}, nil
}

func (s *Service) InitiateUpload(ctx context.Context, request *debuginfopb.InitiateUploadRequest) (*debuginfopb.InitiateUploadResponse, error) {
	should, reason, err := s.store.ShouldUpload(request.GetBuildId(), request.GetType(), request.GetForce())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if !should {
		return nil, status.Error(codes.FailedPrecondition, reason)
	}
	session, err := s.store.Initiate(request.GetBuildId(), request.GetBuildIdType(), request.GetType(), request.GetHash(), request.GetSize(), request.GetForce())
	if err != nil {
		return nil, storeStatus(err)
	}
	return &debuginfopb.InitiateUploadResponse{UploadInstructions: &debuginfopb.UploadInstructions{BuildId: session.BuildID, UploadId: session.ID, Type: session.Type, UploadStrategy: debuginfopb.UploadInstructions_UPLOAD_STRATEGY_GRPC}}, nil
}

func (s *Service) Upload(stream grpc.ClientStreamingServer[debuginfopb.UploadRequest, debuginfopb.UploadResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "receive upload information: %v", err)
	}
	info := first.GetInfo()
	if info == nil {
		return status.Error(codes.InvalidArgument, "first upload message must contain upload information")
	}
	session, err := s.store.Upload(info.GetUploadId(), info.GetBuildId(), info.GetType(), func(writer io.Writer) error {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		for {
			request, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("receive upload chunk: %w", err)
			}
			if request.GetInfo() != nil {
				return fmt.Errorf("upload information may only be sent first")
			}
			chunk := request.GetChunkData()
			if chunk == nil {
				return fmt.Errorf("upload message must contain chunk data")
			}
			if _, err := writer.Write(chunk); err != nil {
				return fmt.Errorf("write upload chunk: %w", err)
			}
		}
	})
	if err != nil {
		return storeStatus(err)
	}
	return stream.SendAndClose(&debuginfopb.UploadResponse{BuildId: session.BuildID, Size: uint64(session.Size)})
}

func (s *Service) MarkUploadFinished(ctx context.Context, request *debuginfopb.MarkUploadFinishedRequest) (*debuginfopb.MarkUploadFinishedResponse, error) {
	if err := s.store.MarkFinished(request.GetBuildId(), request.GetUploadId(), request.GetType()); err != nil {
		return nil, storeStatus(err)
	}
	return &debuginfopb.MarkUploadFinishedResponse{}, nil
}

func storeStatus(err error) error {
	switch {
	case errors.Is(err, ErrTooLarge):
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	case errors.Is(err, ErrNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrAlreadyExists):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case errors.Is(err, ErrNotUploaded), errors.Is(err, ErrSizeMismatch):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
