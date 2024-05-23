package resource_manager

import (
	"context"
	"fmt"
	"net"

	"github.com/google/uuid"
	"github.com/if-nil/tcc-toy/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	commitScript = `
local key = KEYS[1]
local oldValue = ARGV[1]
local newValue = ARGV[2]

if redis.call('exists', key) == 1 and redis.call('get', key) == oldValue then
  redis.call('set', key, newValue)
  return 0
else
  return 1
end
`
	cancelScript = `
local key = KEYS[1]
local oldValue = ARGV[1]
local newValue = ARGV[2]

if redis.call('exists', key) == 0 then
  return 0
elseif redis.call('get', key) == oldValue then
  redis.call('set', key, newValue)
  return 1
else
  return 2
end

`

	tried     = "tried"
	committed = "committed"
	cancelled = "cancelled"
)

type Manager struct {
	port         int
	address      string
	uuid         string
	rdb          *redis.Client
	commitScript *redis.Script
	cancelScript *redis.Script
	pb.ResourceManagerServer
}

// Try 防悬挂
func (m *Manager) Try(ctx context.Context, req *pb.TryRequest) (*pb.TryReply, error) {
	key := fmt.Sprintf("xid:%s:%s", m.uuid, req.Xid)
	isSet, err := m.rdb.SetNX(ctx, key, tried, redis.KeepTTL).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to setnx: %v", err)
	}
	if !isSet {
		return nil, status.Errorf(codes.AlreadyExists, "xid %s already exists", req.Xid)
	}
	return m.ResourceManagerServer.Try(ctx, req)
}

func (m *Manager) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitReply, error) {
	key := fmt.Sprintf("xid:%s:%s", m.uuid, req.Xid)
	res, err := m.commitScript.Run(ctx, m.rdb, []string{key}, tried, committed).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to run commit script: %v", err)
	}
	// 0: 不存在 1: 已提交
	if res.(int64) != 0 {
		return &pb.CommitReply{
			Message: fmt.Sprintf("xid %s already committed", req.Xid),
		}, nil
	}
	return m.ResourceManagerServer.Commit(ctx, req)
}

// Cancel 空补偿
func (m *Manager) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelReply, error) {
	key := fmt.Sprintf("xid:%s:%s", m.uuid, req.Xid)
	res, err := m.cancelScript.Run(ctx, m.rdb, []string{key}, tried, cancelled).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to run commit script: %v", err)
	}
	switch res.(int64) {
	case 0:
		return &pb.CancelReply{Message: "tx is not exist"}, nil
	case 1:
		return m.ResourceManagerServer.Cancel(ctx, req)
	case 2:
		return &pb.CancelReply{
			Message: fmt.Sprintf("xid %s already committed", req.Xid),
		}, nil
	}
	return nil, nil
}

type Option func(*Manager)

func WithPort(port int) Option {
	return func(m *Manager) {
		m.port = port
	}
}

func WithAddress(address string) Option {
	return func(m *Manager) {
		m.address = address
	}
}

func New(server pb.ResourceManagerServer, redisCli *redis.Client, opts ...Option) *Manager {
	m := &Manager{
		port:                  9020,
		address:               "localhost",
		rdb:                   redisCli,
		commitScript:          redis.NewScript(commitScript),
		cancelScript:          redis.NewScript(cancelScript),
		uuid:                  uuid.New().String(),
		ResourceManagerServer: server,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *Manager) Run() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", m.address, m.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterResourceManagerServer(s, m)
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
