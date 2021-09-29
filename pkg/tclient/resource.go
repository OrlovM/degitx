// MIT License. Copyright (c) 2020 CQFN
// https://github.com/cqfn/degitx/blob/master/LICENSE

package tclient

import (
	"context"

	pb "cqfn.org/degitx/proto/go/tcommitpb"
)

// ResourceClient implements tcommit.Resource interface.
// It is assumed to be a part of the transaction manager
// for remote procedure calls.
type ResourceClient struct {
	client pb.ResourceServiceClient
}

// NewResourceClient with a given gRPC client
func NewResourceClient(grpcClient pb.ResourceServiceClient) *ResourceClient {
	return &ResourceClient{grpcClient}
}

// Commit transaction RPC
func (r *ResourceClient) Commit(ctx context.Context) error {
	req := new(pb.CommitRequest)
	_, err := r.client.Commit(ctx, req)
	return err
}

// Abort transaction RPC
func (r *ResourceClient) Abort(ctx context.Context) error {
	req := new(pb.AbortRequest)
	_, err := r.client.Abort(ctx, req)
	return err
}
