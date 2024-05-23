package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/if-nil/tcc-toy/pb"
	"github.com/if-nil/tcc-toy/resource_manager"
	"github.com/redis/go-redis/v9"
)

type MockResourceManager struct {
	pb.UnimplementedResourceManagerServer
}

func (manager *MockResourceManager) Try(_ context.Context, req *pb.TryRequest) (*pb.TryReply, error) {
	log.Printf("try -- xid: %s, param: %s\n", req.Xid, req.Param)
	// Mock random error during try phase
	if rand.Intn(10)%2 == 0 {
		return &pb.TryReply{}, errors.New("random error")
	}
	return &pb.TryReply{}, nil
}

func (manager *MockResourceManager) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitReply, error) {
	log.Printf("commit -- xid: %s\n", req.Xid)
	return &pb.CommitReply{}, nil
}

func (manager *MockResourceManager) Cancel(_ context.Context, req *pb.CancelRequest) (*pb.CancelReply, error) {
	log.Printf("cancel -- xid: %s\n", req.Xid)
	return &pb.CancelReply{}, nil
}

func main() {
	redisCli := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	wg := sync.WaitGroup{}
	resourceManager := func(port int) {
		defer wg.Done()
		//start a resource manager server
		mock := &MockResourceManager{}
		rm := resource_manager.New(mock, redisCli, resource_manager.WithPort(port))
		fmt.Printf("resource manager server on port %d is running", port)

		if err := rm.Run(); err != nil {
			fmt.Printf("resource manager on port %d failed: %v", port, err)
			return
		}
	}
	wg.Add(2)
	// Start 2 resource manager server
	go resourceManager(9210)
	go resourceManager(9211)
	wg.Wait()
}
