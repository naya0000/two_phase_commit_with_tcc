package transaction_manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/if-nil/tcc-toy/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transaction struct {
	xid       string // transaction id
	ctx       context.Context
	resources []pb.ResourceManagerClient
}

type Operation int

const (
	commit Operation = iota
	cancel
)

func (t *Transaction) CallTry(host string, req *pb.TryRequest) (*pb.TryReply, error) {
	// Create a connection to the resource manager
	conn, err := grpc.Dial(host, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	// Create a resource manager client
	resourceClient := pb.NewResourceManagerClient(conn)
	t.resources = append(t.resources, resourceClient)
	req.Xid = t.xid
	return resourceClient.Try(t.ctx, req)
}

func (t *Transaction) Commit() Operation {
	return commit
}

func (t *Transaction) Cancel() Operation {
	return cancel
}

func (t *Transaction) commit() {
	wg := sync.WaitGroup{}
	retryResources := make([]pb.ResourceManagerClient, 0)
	resources := t.resources
	for {
		rwLock := sync.RWMutex{}
		for i := range resources {
			r := t.resources[i]
			wg.Add(1)
			go func(r pb.ResourceManagerClient) {
				defer wg.Done()
				_, err := r.Commit(t.ctx, &pb.CommitRequest{Xid: t.xid})
				rwLock.Lock()
				if err != nil {
					retryResources = append(retryResources, r)
				}
				rwLock.Unlock()
			}(r)
		}
		wg.Wait()
		if len(retryResources) > 0 {
			resources = retryResources
			retryResources = make([]pb.ResourceManagerClient, 0)
			t.backoff()
		} else {
			break
		}
	}
}

func (t *Transaction) cancel() {
	wg := sync.WaitGroup{}
	retryResources := make([]pb.ResourceManagerClient, 0)
	resources := t.resources
	for {
		rwLock := sync.RWMutex{}
		for i := range resources {
			r := t.resources[i]
			wg.Add(1)
			go func(r pb.ResourceManagerClient) {
				defer wg.Done()
				_, err := r.Cancel(t.ctx, &pb.CancelRequest{Xid: t.xid})
				rwLock.Lock()
				if err != nil {
					retryResources = append(retryResources, r)
				}
				rwLock.Unlock()
			}(r)
		}
		wg.Wait()
		if len(retryResources) > 0 {
			resources = retryResources
			retryResources = make([]pb.ResourceManagerClient, 0)
			t.backoff()
		} else {
			break
		}
	}
}

func (t *Transaction) backoff() {
	time.Sleep(time.Second)
}

func TCCCall(txFunc func(*Transaction) Operation) (err error) {
	t := &Transaction{
		xid:       uuid.New().String(),
		ctx:       context.Background(),
		resources: make([]pb.ResourceManagerClient, 0),
	}
	o := txFunc(t)
	if o == commit {
		t.commit()
	} else if o == cancel {
		t.cancel()
	} else {
		t.cancel()
		err = fmt.Errorf("unknown operation %v", o)
	}
	return
}
