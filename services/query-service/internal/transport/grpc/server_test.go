package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	"github.com/bytepharaoh/infrapilot/pkg/grpcmeta"
)

const (
	org     = "019a2a0b-707b-7f52-a225-ae9e77567f94"
	project = "019a2a0b-707b-7f52-a225-ae9e77567f95"
)

type authClient struct {
	cpv1.AuthorizationServiceClient
	allowed bool
	calls   int
}

func (a *authClient) CheckPermission(ctx context.Context, _ *cpv1.CheckPermissionRequest, _ ...grpc.CallOption) (*cpv1.CheckPermissionResponse, error) {
	a.calls++
	if len(metadata.ValueFromOutgoingContext(ctx, grpcmeta.Principal)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "principal missing downstream")
	}
	return &cpv1.CheckPermissionResponse{Allowed: a.allowed}, nil
}

type streamClient struct {
	streamv1.StreamReadServiceClient
	calls int
}

func (s *streamClient) ListCheckResults(ctx context.Context, _ *streamv1.ListCheckResultsRequest, _ ...grpc.CallOption) (*streamv1.ListCheckResultsResponse, error) {
	s.calls++
	if len(metadata.ValueFromOutgoingContext(ctx, grpcmeta.Principal)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "principal missing downstream")
	}
	return &streamv1.ListCheckResultsResponse{}, nil
}

func TestQueryRequiresPrincipalAndCurrentProjectPermission(t *testing.T) {
	identity, err := proto.Marshal(&cpv1.Principal{Kind: cpv1.PrincipalKind_PRINCIPAL_KIND_USER, Subject: "subject"})
	if err != nil {
		t.Fatal(err)
	}
	auth := &authClient{}
	stream := &streamClient{}
	server := &Server{Auth: auth, Stream: stream}
	req := &streamv1.ListCheckResultsRequest{OrganizationId: org, ProjectId: project}
	if _, err := server.ListCheckResults(context.Background(), req); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing principal: %v", err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(grpcmeta.Principal, string(identity)))
	if _, err := server.ListCheckResults(ctx, req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("denied principal: %v", err)
	}
	if auth.calls != 1 || stream.calls != 0 {
		t.Fatalf("denied request reached stream: auth=%d stream=%d", auth.calls, stream.calls)
	}
	auth.allowed = true
	if _, err := server.ListCheckResults(ctx, req); err != nil {
		t.Fatal(err)
	}
	if auth.calls != 2 || stream.calls != 1 {
		t.Fatalf("authorized request not forwarded: auth=%d stream=%d", auth.calls, stream.calls)
	}
}
