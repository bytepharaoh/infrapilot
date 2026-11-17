// Package grpc implements the tenant-authorized query boundary for stream reads.
package grpc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	"github.com/bytepharaoh/infrapilot/pkg/grpcmeta"
)

// Server delegates bounded reads only after a fresh project permission check.
type Server struct {
	streamv1.UnimplementedStreamReadServiceServer
	Auth   cpv1.AuthorizationServiceClient
	Stream streamv1.StreamReadServiceClient
}

func canonicalID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func (s *Server) authorize(ctx context.Context, orgID, projectID string) (context.Context, error) {
	if !canonicalID(orgID) || !canonicalID(projectID) {
		return nil, status.Error(codes.InvalidArgument, "organization and project must be canonical UUIDs")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	principal := md.Get(grpcmeta.Principal)
	if len(principal) != 1 || len(principal[0]) == 0 || len(principal[0]) > 4096 {
		return nil, status.Error(codes.Unauthenticated, "missing principal")
	}
	var identity cpv1.Principal
	if err := proto.Unmarshal([]byte(principal[0]), &identity); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid principal")
	}
	if identity.GetKind() != cpv1.PrincipalKind_PRINCIPAL_KIND_USER && identity.GetKind() != cpv1.PrincipalKind_PRINCIPAL_KIND_API_KEY {
		return nil, status.Error(codes.Unauthenticated, "invalid principal kind")
	}
	bound, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	delegated := metadata.NewOutgoingContext(bound, metadata.Pairs(grpcmeta.Principal, principal[0]))
	allowed, err := s.Auth.CheckPermission(delegated, &cpv1.CheckPermissionRequest{
		OrganizationId: orgID, ProjectId: projectID, Permission: cpv1.Permission_PERMISSION_RESOURCE_READ,
	})
	if err != nil {
		return nil, err
	}
	if !allowed.GetAllowed() {
		return nil, status.Error(codes.PermissionDenied, "project read is forbidden")
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(grpcmeta.Principal, principal[0])), nil
}

func (s *Server) ListCheckResults(ctx context.Context, req *streamv1.ListCheckResultsRequest) (*streamv1.ListCheckResultsResponse, error) {
	delegated, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId())
	if err != nil {
		return nil, err
	}
	bound, cancel := context.WithTimeout(delegated, 4*time.Second)
	defer cancel()
	return s.Stream.ListCheckResults(bound, req)
}

func (s *Server) ListMetrics(ctx context.Context, req *streamv1.ListMetricsRequest) (*streamv1.ListMetricsResponse, error) {
	delegated, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId())
	if err != nil {
		return nil, err
	}
	bound, cancel := context.WithTimeout(delegated, 4*time.Second)
	defer cancel()
	return s.Stream.ListMetrics(bound, req)
}
