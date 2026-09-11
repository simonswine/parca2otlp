package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	profilestoregrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/profilestore/v1alpha1/profilestorev1alpha1grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/simonswine/parca2otlp/internal/profilestore"
	"github.com/simonswine/parca2otlp/internal/storage"
)

func main() {
	listenAddress := flag.String("listen-address", ":7070", "gRPC listen address")
	storagePath := flag.String("storage-path", "", "directory for OTLP Profiles protobuf files (required)")
	maxStorageSize := flag.String("max-storage-size", "256MiB", "maximum managed OTLP storage; 0 disables retention")
	maxReceiveSize := flag.Int("max-recv-message-size", 64<<20, "maximum gRPC request size in bytes")
	flag.Parse()

	storageLimit, err := storage.ParseSize(*maxStorageSize)
	if err != nil {
		log.Fatal(err)
	}
	store, err := storage.New(*storagePath, storageLimit)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", *listenAddress, err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(*maxReceiveSize))
	profilestoregrpc.RegisterProfileStoreServiceServer(server, profilestore.New(store))
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)

	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	log.Printf("parca2otlp listening on %s, writing to %s", *listenAddress, *storagePath)

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
