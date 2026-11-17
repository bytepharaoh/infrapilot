// Package domain defines stored stream records and threshold evaluation.
package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
)

const MaxRange = 7 * 24 * time.Hour

// Scope identifies a resource within its owning tenant.
type Scope struct {
	OrganizationID, ProjectID, ResourceID uuid.UUID
}

// Check is an independently replayable scheduled result.
type Check struct {
	Scope
	CheckID                 uuid.UUID
	ScheduledAt, ObservedAt time.Time
	LeaseEpoch              int64
	Succeeded               bool
	DurationMs              float64
	Payload                 []byte
	Result                  *checksv1.CheckResult
	TraceParent             string
}

// Metric holds a validated sample, with its original payload available for reads.
type Metric struct {
	Scope
	Name               string
	ObservedAt         time.Time
	SeriesID, SampleID []byte
	Scalar             *float64
	Payload            []byte
}

// Threshold encodes one threshold from a capture of the check definition.
type Threshold struct {
	Kind                   string
	Applied, Known, Firing bool
	Observed, Limit        float64
}

// Thresholds evaluates both supported thresholds. An unavailable TLS observation leaves
// the existing state unchanged because the new check says nothing about certificate age.
func Thresholds(result *checksv1.CheckResult) [2]Threshold {
	duration := Threshold{Kind: "duration_ms", Known: true, Observed: result.GetDurationMs()}
	tls := Threshold{Kind: "tls_days_remaining", Known: true}
	if definition := result.GetThresholds(); definition != nil {
		if definition.MaxDurationMs != nil {
			duration.Applied = true
			duration.Limit = definition.GetMaxDurationMs()
			duration.Firing = duration.Observed > duration.Limit
		}
		if definition.MinTlsDays != nil {
			tls.Applied = true
			tls.Known = result.TlsDaysRemaining != nil
			tls.Limit = float64(definition.GetMinTlsDays())
			tls.Observed = float64(result.GetTlsDaysRemaining())
			tls.Firing = tls.Known && tls.Observed < tls.Limit
		}
	}
	return [2]Threshold{duration, tls}
}

// ParseID accepts the canonical UUID representation accepted in message keys.
func ParseID(raw, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil || id.String() != raw {
		return uuid.Nil, fmt.Errorf("%s must be a canonical nonzero UUID", field)
	}
	return id, nil
}

func ParseScope(org, project, resource string) (Scope, error) {
	o, orgErr := ParseID(org, "organization_id")
	p, projectErr := ParseID(project, "project_id")
	r, resourceErr := ParseID(resource, "resource_id")
	return Scope{OrganizationID: o, ProjectID: p, ResourceID: r}, errors.Join(orgErr, projectErr, resourceErr)
}
