// Package grpc exposes tenant-scoped check definitions to authenticated gateway peers.
package grpc

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	"github.com/bytepharaoh/infrapilot/pkg/grpcmeta"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store/db"
)

// Server checks permissions against the live control-plane before reading or mutating checks.
type Server struct {
	checksv1.UnimplementedCheckServiceServer
	store     *store.Store
	cp        cpv1.AuthorizationServiceClient
	resources cpv1.ResourceServiceClient
	log       *slog.Logger
}

// New binds the service to the two control-plane read RPCs it requires.
func New(checks *store.Store, conn *gogrpc.ClientConn, logger *slog.Logger) *Server {
	return &Server{store: checks, cp: cpv1.NewAuthorizationServiceClient(conn), resources: cpv1.NewResourceServiceClient(conn), log: logger}
}

type consumer struct {
	ctx       context.Context //nolint:containedctx // Request-scoped carrier for the authorized RPC only.
	scope     store.Scope
	principal string
}

func parseUUID(text string) (uuid.UUID, bool) {
	id, err := uuid.Parse(text)
	return id, err == nil && id != uuid.Nil && id.String() == text
}

func (s *Server) authorize(ctx context.Context, orgID, projectID string, permission cpv1.Permission) (consumer, error) {
	org, okOrg := parseUUID(orgID)
	project, okProject := parseUUID(projectID)
	if !okOrg || !okProject {
		return consumer{}, status.Error(codes.InvalidArgument, "organization and project must be canonical UUIDs")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	principal := md.Get(grpcmeta.Principal)
	if len(principal) != 1 || len(principal[0]) > 4096 {
		return consumer{}, status.Error(codes.Unauthenticated, "missing or invalid principal")
	}
	var p cpv1.Principal
	if err := proto.Unmarshal([]byte(principal[0]), &p); err != nil {
		return consumer{}, status.Error(codes.Unauthenticated, "invalid principal")
	}
	var principalKey string
	switch p.GetKind() {
	case cpv1.PrincipalKind_PRINCIPAL_KIND_USER:
		if p.GetSubject() == "" || len(p.GetSubject()) > 255 {
			return consumer{}, status.Error(codes.Unauthenticated, "invalid user principal")
		}
		principalKey = "user/" + p.GetSubject()
	case cpv1.PrincipalKind_PRINCIPAL_KIND_API_KEY:
		if _, ok := parseUUID(p.GetApiKeyId()); !ok {
			return consumer{}, status.Error(codes.Unauthenticated, "invalid API key principal")
		}
		principalKey = "key/" + p.GetApiKeyId()
	default:
		return consumer{}, status.Error(codes.Unauthenticated, "invalid principal kind")
	}
	outgoing := metadata.NewOutgoingContext(ctx, metadata.Pairs(grpcmeta.Principal, principal[0]))
	answer, err := s.cp.CheckPermission(outgoing, &cpv1.CheckPermissionRequest{OrganizationId: org.String(), ProjectId: project.String(), Permission: permission})
	if err != nil {
		return consumer{}, err
	}
	if !answer.GetAllowed() {
		return consumer{}, status.Error(codes.PermissionDenied, "operation is forbidden for this project")
	}
	return consumer{ctx: outgoing, scope: store.Scope{Organization: org, Project: project}, principal: principalKey}, nil
}

func (s *Server) resource(c consumer, resource uuid.UUID) error {
	_, err := s.resources.GetResource(c.ctx, &cpv1.GetResourceRequest{OrganizationId: c.scope.Organization.String(),
		ProjectId: c.scope.Project.String(), ResourceId: resource.String()})
	return err
}

func definition(spec *checksv1.CheckSpec) (domain.Definition, error) {
	id, ok := parseUUID(spec.GetResourceId())
	if !ok || spec.GetPort() > 65535 || spec.GetIntervalSeconds() > 86400 || spec.GetTimeoutSeconds() > 15 {
		return domain.Definition{}, status.Error(codes.InvalidArgument, "resource, cadence, or timeout is invalid")
	}
	definition := domain.Definition{Resource: id, Kind: domain.Kind(spec.GetKind()), Target: spec.GetTarget(), Port: int32(spec.GetPort()),
		IntervalSeconds: int32(spec.GetIntervalSeconds()), TimeoutSeconds: int32(spec.GetTimeoutSeconds()), Enabled: spec.GetEnabled()}
	if spec.GetKind() < checksv1.CheckKind_CHECK_KIND_HTTP || spec.GetKind() > checksv1.CheckKind_CHECK_KIND_ICMP {
		return domain.Definition{}, status.Error(codes.InvalidArgument, "invalid check kind")
	}
	if thresholds := spec.GetThresholds(); thresholds != nil {
		definition.MaxDurationMS = thresholds.MaxDurationMs
		if thresholds.MinTlsDays != nil {
			if thresholds.GetMinTlsDays() > 365 {
				return domain.Definition{}, status.Error(codes.InvalidArgument, "minimum TLS days exceeds 365")
			}
			days := int32(thresholds.GetMinTlsDays())
			definition.MinTLSDays = &days
		}
	}
	if err := definition.Validate(); err != nil {
		return domain.Definition{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return definition, nil
}

func toProto(row db.Check) *checksv1.Check {
	d := store.Definition(row)
	thresholds := &checksv1.Thresholds{MaxDurationMs: d.MaxDurationMS}
	if d.MinTLSDays != nil && *d.MinTLSDays > 0 {
		days := uint32(*d.MinTLSDays)
		thresholds.MinTlsDays = &days
	}
	return &checksv1.Check{Id: row.ID.String(), OrganizationId: row.OrganizationID.String(), ProjectId: row.ProjectID.String(),
		CreatedAt: timestamppb.New(row.CreatedAt), UpdatedAt: timestamppb.New(row.UpdatedAt), Spec: &checksv1.CheckSpec{
			ResourceId: d.Resource.String(), Kind: checksv1.CheckKind(d.Kind), Target: d.Target,
			Port: uint32(max(0, d.Port)), IntervalSeconds: uint32(max(0, d.IntervalSeconds)), TimeoutSeconds: uint32(max(0, d.TimeoutSeconds)),
			Enabled: d.Enabled, Thresholds: thresholds,
		}}
}

func (s *Server) errorStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return status.Error(codes.NotFound, "check does not exist")
	case errors.Is(err, store.ErrDefinitionLimit):
		return status.Error(codes.ResourceExhausted, "organization check limit reached")
	case errors.Is(err, store.ErrKeyBusy):
		return status.Error(codes.AlreadyExists, "check create is in progress")
	case errors.Is(err, store.ErrKeyReused):
		st := status.New(codes.AlreadyExists, "idempotency key already used for different settings")
		withReason, detailErr := st.WithDetails(&errdetails.ErrorInfo{Reason: "IDEMPOTENCY_KEY_REUSED", Domain: "infrapilot"})
		if detailErr != nil {
			return st.Err()
		}
		return withReason.Err()
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		s.log.ErrorContext(ctx, "check storage operation failed", slog.Any("error", err))
		return status.Error(codes.Internal, "check storage operation failed")
	}
}

// CreateCheck validates and stores a check in the caller's project.
func (s *Server) CreateCheck(ctx context.Context, req *checksv1.CreateCheckRequest) (*checksv1.CreateCheckResponse, error) {
	c, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId(), cpv1.Permission_PERMISSION_RESOURCE_WRITE)
	if err != nil {
		return nil, err
	}
	d, err := definition(req.GetSpec())
	if err != nil {
		return nil, err
	}
	key := req.GetIdempotencyKey()
	if len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < 32 || r > 126 }) {
		return nil, status.Error(codes.InvalidArgument, "idempotency key must contain 1 to 128 printable ASCII characters")
	}
	if err := s.resource(c, d.Resource); err != nil {
		return nil, err
	}
	row, err := s.store.Create(ctx, c.scope, d, store.Idempotency{Principal: c.principal, Key: key})
	if err != nil {
		return nil, s.errorStatus(ctx, err)
	}
	return &checksv1.CreateCheckResponse{Check: toProto(row)}, nil
}

// GetCheck reads one check by its canonical ID.
func (s *Server) GetCheck(ctx context.Context, req *checksv1.GetCheckRequest) (*checksv1.GetCheckResponse, error) {
	c, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId(), cpv1.Permission_PERMISSION_RESOURCE_READ)
	if err != nil {
		return nil, err
	}
	id, ok := parseUUID(req.GetCheckId())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "check ID must be a canonical UUID")
	}
	row, err := s.store.Get(ctx, c.scope, id)
	if err != nil {
		return nil, s.errorStatus(ctx, err)
	}
	return &checksv1.GetCheckResponse{Check: toProto(row)}, nil
}

// ListChecks returns a bounded page from the authorized project.
func (s *Server) ListChecks(ctx context.Context, req *checksv1.ListChecksRequest) (*checksv1.ListChecksResponse, error) {
	c, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId(), cpv1.Permission_PERMISSION_RESOURCE_READ)
	if err != nil {
		return nil, err
	}
	size := req.GetPageSize()
	if size < 0 || size > 200 {
		return nil, status.Error(codes.InvalidArgument, "page size must be between 1 and 200")
	}
	if size == 0 {
		size = 50
	}
	var after uuid.UUID
	if token := req.GetPageToken(); token != "" {
		if len(token) != 22 {
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}
		raw, decodeErr := base64.RawURLEncoding.DecodeString(token)
		if decodeErr != nil || len(raw) != len(after) {
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}
		copy(after[:], raw)
	}
	rows, err := s.store.List(ctx, c.scope, after, size+1)
	if err != nil {
		return nil, s.errorStatus(ctx, err)
	}
	response := &checksv1.ListChecksResponse{}
	if len(rows) > int(size) {
		rows = rows[:size]
		response.NextPageToken = base64.RawURLEncoding.EncodeToString(rows[len(rows)-1].ID[:])
	}
	response.Checks = make([]*checksv1.Check, 0, len(rows))
	for _, row := range rows {
		response.Checks = append(response.Checks, toProto(row))
	}
	return response, nil
}

// UpdateCheck replaces a check's configuration and fences its previous lease.
func (s *Server) UpdateCheck(ctx context.Context, req *checksv1.UpdateCheckRequest) (*checksv1.UpdateCheckResponse, error) {
	c, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId(), cpv1.Permission_PERMISSION_RESOURCE_WRITE)
	if err != nil {
		return nil, err
	}
	id, ok := parseUUID(req.GetCheckId())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "check ID must be a canonical UUID")
	}
	d, err := definition(req.GetSpec())
	if err != nil {
		return nil, err
	}
	if err := s.resource(c, d.Resource); err != nil {
		return nil, err
	}
	row, err := s.store.Update(ctx, c.scope, id, d)
	if err != nil {
		return nil, s.errorStatus(ctx, err)
	}
	return &checksv1.UpdateCheckResponse{Check: toProto(row)}, nil
}

// DeleteCheck permanently removes a check from its authorized project.
func (s *Server) DeleteCheck(ctx context.Context, req *checksv1.DeleteCheckRequest) (*checksv1.DeleteCheckResponse, error) {
	c, err := s.authorize(ctx, req.GetOrganizationId(), req.GetProjectId(), cpv1.Permission_PERMISSION_RESOURCE_WRITE)
	if err != nil {
		return nil, err
	}
	id, ok := parseUUID(req.GetCheckId())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "check ID must be a canonical UUID")
	}
	if err := s.store.Delete(ctx, c.scope, id); err != nil {
		return nil, s.errorStatus(ctx, err)
	}
	return &checksv1.DeleteCheckResponse{}, nil
}
