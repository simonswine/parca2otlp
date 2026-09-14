package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	debuginfogrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/debuginfo/v1alpha1/debuginfov1alpha1grpc"
	profilestoregrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/profilestore/v1alpha1/profilestorev1alpha1grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/simonswine/parca2otlp/internal/debuginfo"
	"github.com/simonswine/parca2otlp/internal/profilestore"
	"github.com/simonswine/parca2otlp/internal/storage"
)

func main() {
	listenAddress := flag.String("listen-address", ":7070", "gRPC listen address")
	storagePath := flag.String("storage-path", "", "directory for OTLP Profiles protobuf files (required)")
	maxStorageSize := flag.String("max-storage-size", "256MiB", "maximum managed OTLP storage; 0 disables retention")
	debuginfoStoragePath := flag.String("debuginfo-storage-path", "", "directory for Parca debug artifacts; defaults to <storage-path>/debuginfo")
	maxDebuginfoStorageSize := flag.String("max-debuginfo-storage-size", "256MiB", "maximum managed debug artifact storage; 0 disables retention")
	debuginfoUploadStaleAfter := flag.Duration("debuginfo-upload-stale-after", 15*time.Minute, "age after which an unfinished debug upload can be replaced")
	maxReceiveSize := flag.Int("max-recv-message-size", 64<<20, "maximum gRPC request size in bytes")
	flag.Parse()

	storageLimit, err := storage.ParseSize(*maxStorageSize)
	if err != nil {
		log.Fatal(err)
	}
	debuginfoLimit, err := storage.ParseSize(*maxDebuginfoStorageSize)
	if err != nil {
		log.Fatal(err)
	}
	store, err := storage.New(*storagePath, storageLimit)
	if err != nil {
		log.Fatal(err)
	}
	if *debuginfoStoragePath == "" {
		*debuginfoStoragePath = filepath.Join(*storagePath, "debuginfo")
	}
	debuginfoStore, err := debuginfo.New(*debuginfoStoragePath, debuginfoLimit, *debuginfoUploadStaleAfter)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", *listenAddress, err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(*maxReceiveSize))
	profilestoregrpc.RegisterProfileStoreServiceServer(server, profilestore.New(store))
	debuginfogrpc.RegisterDebuginfoServiceServer(server, debuginfo.NewService(debuginfoStore))
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)

	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	log.Printf("parca2otlp listening on %s, profiles in %s, debug artifacts in %s", *listenAddress, *storagePath, *debuginfoStoragePath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		server.GracefulStop()
	case err := <-errCh:
		if err != nil {
			log.Fatal(fmt.Errorf("serve: %w", err))
		}
	}
}
