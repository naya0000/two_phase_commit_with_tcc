package transaction

import (
	"context"
	"fmt"
	"github.com/if-nil/tcc-toy/resource_manager"
	"github.com/if-nil/tcc-toy/resource_manager/pb"
	"github.com/if-nil/tcc-toy/transaction_manager"
	"github.com/redis/go-redis/v9"
	"sync"
	"testing"
	"time"
)

type MockResourceManager struct {
	pb.UnimplementedResourceManagerServer
}

func (manager *MockResourceManager) Try(_ context.Context, req *pb.TryRequest) (*pb.TryReply, error) {
	fmt.Printf("try -- xid: %s, param: %s\n", req.Xid, req.Param)
	return &pb.TryReply{}, nil
}

func (manager *MockResourceManager) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitReply, error) {
	fmt.Printf("commit -- xid: %s\n", req.Xid)
	return &pb.CommitReply{}, nil
}

func (manager *MockResourceManager) Cancel(_ context.Context, req *pb.CancelRequest) (*pb.CancelReply, error) {
	fmt.Printf("cancel -- xid: %s\n", req.Xid)
	return &pb.CancelReply{}, nil
}

func TestTransaction(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{Addr: "mydomain.com:6379", Password: "123456"})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		// 启动一个resource manager
		mock := &MockResourceManager{}
		rm := resource_manager.New(mock, redisCli, resource_manager.WithPort(9210))
		wg.Done()
		if err := rm.Run(); err != nil {
			t.Error(err)
			return
		}
	}()
	wg.Add(1)
	go func() {
		// 启动一个resource manager
		mock := &MockResourceManager{}
		rm := resource_manager.New(mock, redisCli, resource_manager.WithPort(9211))
		wg.Done()
		if err := rm.Run(); err != nil {
			t.Error(err)
			return
		}
	}()

	wg.Wait()
	time.Sleep(time.Second)
	// 启动一个transaction manager
	err := transaction_manager.TCCCall(func(tx *transaction_manager.Transaction) transaction_manager.Operation {
		_, err := tx.CallTry("localhost:9210", &pb.TryRequest{Param: "tx1"})
		if err != nil {
			t.Errorf("tx1 try failed: %v", err)
			return tx.Cancel()
		}
		_, err = tx.CallTry("localhost:9211", &pb.TryRequest{Param: "tx2"})
		if err != nil {
			t.Errorf("tx2 try failed: %v", err)
			return tx.Cancel()
		}
		t.Logf("tx1 and tx2 try success")
		return tx.Commit()
	})
	if err != nil {
		t.Fatal(err)
	}
}
