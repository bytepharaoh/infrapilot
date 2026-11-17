package grpc

import (
	"slices"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
)

// PeerRules separates the user-facing control-plane APIs from the telemetry credential check.
func PeerRules(edge, ingestion []string, delegated ...string) map[string][]string {
	edgeOnly := []string{
		cpv1.OrganizationService_ServiceDesc.ServiceName,
		cpv1.ProjectService_ServiceDesc.ServiceName,
		cpv1.MemberService_ServiceDesc.ServiceName,
		cpv1.ResourceService_ServiceDesc.ServiceName,
		cpv1.ApiKeyService_ServiceDesc.ServiceName,
		cpv1.AuditService_ServiceDesc.ServiceName,
	}
	rules := make(map[string][]string, len(edgeOnly)+2)
	for _, svc := range edgeOnly {
		rules["/"+svc+"/*"] = edge
	}
	rules["/"+cpv1.AuthorizationService_ServiceDesc.ServiceName+"/*"] = edge
	if len(ingestion) != 0 {
		rules[cpv1.IngestionAuthorizationService_ValidateIngestResource_FullMethodName] = ingestion
	}
	if len(delegated) > 0 {
		readers := slices.Concat(edge, delegated)
		rules[cpv1.AuthorizationService_CheckPermission_FullMethodName] = readers
		rules[cpv1.ResourceService_GetResource_FullMethodName] = readers
	}
	return rules
}

func delegatedMethod(fullMethod string) bool {
	return fullMethod == cpv1.AuthorizationService_CheckPermission_FullMethodName ||
		fullMethod == cpv1.ResourceService_GetResource_FullMethodName
}
