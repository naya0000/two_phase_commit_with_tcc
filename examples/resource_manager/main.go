package main

import (
	"context"
	"errors"
	"github.com/if-nil/tcc-toy/pb"
	"github.com/if-nil/tcc-toy/resource_manager"
	"github.com/redis/go-redis/v9"
	"log"
	"math/rand"
	"sync"
)

type MockResourceManager struct {
	pb.UnimplementedResourceManagerServer
}

func (manager *MockResourceManager) Try(_ context.Context, req *pb.TryRequest) (*pb.TryReply, error) {
	log.Printf("try -- xid: %s, param: %s\n", req.Xid, req.Param)
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
	redisCli := redis.NewClient(&redis.Options{Addr: "mydomain.com:6379", Password: "123456"})
	wg := sync.WaitGroup{}
	resourceManager := func(port int) {
		defer wg.Done()
		// 启动一个resource manager
		mock := &MockResourceManager{}
		rm := resource_manager.New(mock, redisCli, resource_manager.WithPort(port))
		if err := rm.Run(); err != nil {
			log.Printf("resource manager failed: %v", err)
			return
		}
	}
	wg.Add(2)
	go resourceManager(9210)
	go resourceManager(9211)
	wg.Wait()
}
