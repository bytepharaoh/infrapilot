//go:build integration

package app_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/testutil/certtest"
	"github.com/bytepharaoh/infrapilot/pkg/testutil/pgtest"
)

type tenant struct {
	org      uuid.UUID
	project  uuid.UUID
	resource uuid.UUID
	key      string
}

func controlPlane(t *testing.T) (config.GRPCClient, *pgtest.DB) {
	t.Helper()
	pg := pgtest.Start(t)
	const cpService = "control-plane"
	ca := certtest.NewCA(t)
	cpIdentity := certtest.SPIFFE("integration", cpService)
	ingestIdentity := certtest.SPIFFE("integration", "telemetry-ingestion")
	cpCert := ca.Issue(cpService, cpIdentity)
	receiverCert := ca.Issue("telemetry-ingestion", ingestIdentity)
	grpcEndpoint := freeAddress(t)
	adminEndpoint := freeAddress(t)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), cpService)
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/control-plane")
	build.Dir = filepath.Join("..", "..", "..", cpService)
	build.Env = append(os.Environ(), "GOWORK=off")
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	commonEnv := append(os.Environ(), "SERVICE_NAME="+cpService, "ENVIRONMENT=test", "OTEL_EXPORTER_OTLP_ENDPOINT=",
		"TELEMETRY_METRICS_PUSH=false", "TELEMETRY_LOGS_PUSH=false", "KAFKA_BROKERS=", "SHUTDOWN_DELAY=0s")
	migrate := exec.CommandContext(ctx, binary, "migrate")
	migrate.Env = slices.Concat(commonEnv, []string{"MIGRATION_DATABASE_URL=" + pg.MigratorURL})
	output, err = migrate.CombinedOutput()
	require.NoError(t, err, "%s", output)

	serverCtx, stopServer := context.WithCancel(t.Context())
	t.Cleanup(stopServer)
	server := exec.CommandContext(serverCtx, binary, "serve")
	server.Env = slices.Concat(commonEnv, []string{"DATABASE_URL=" + pg.AppURL, "ADMIN_ADDR=" + adminEndpoint, "GRPC_ADDR=" + grpcEndpoint,
		"GRPC_TLS_CERT_FILE=" + cpCert.Cert, "GRPC_TLS_KEY_FILE=" + cpCert.Key, "GRPC_TLS_CLIENT_CA_FILE=" + ca.File,
		"EDGE_IDENTITIES=" + certtest.SPIFFE("integration", "api-gateway"), "INGESTION_IDENTITIES=" + ingestIdentity})
	server.Stdout, server.Stderr = os.Stderr, os.Stderr
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		stopServer()
		_ = server.Wait()
	})
	waitReady(t, "http://"+adminEndpoint+"/readyz")

	return config.GRPCClient{
		Target: grpcEndpoint, ServerName: "localhost", ServerIdentity: cpIdentity, Timeout: 2 * time.Second,
		TLSCAFile: ca.File, TLSCertFile: receiverCert.Cert, TLSKeyFile: receiverCert.Key,
	}, pg
}

func seedTenant(t *testing.T, pg *pgtest.DB, scopes []string) tenant {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pg.MigratorURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	id := tenant{org: uuid.New(), project: uuid.New(), resource: uuid.New()}
	keyID := uuid.New()
	var secret [32]byte
	_, err = rand.Read(secret[:])
	require.NoError(t, err)
	id.key = "ipk_" + base64.RawURLEncoding.EncodeToString(secret[:])
	hash := sha256.Sum256([]byte(id.key))
	_, err = conn.Exec(ctx, "INSERT INTO organizations(id, name) VALUES($1, 'Integration')", id.org)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO projects(id, organization_id, name, slug) VALUES($1,$2,'Test project','test-project')", id.project, id.org)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO resources(id, organization_id, project_id, kind, name) VALUES($1,$2,$3,'host','test-host')", id.resource, id.org, id.project)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO api_keys(id, organization_id, project_id, name, prefix, scopes, created_by)
		VALUES($1,$2,$3,'Integration',$4,$5,$6)`, keyID, id.org, id.project, id.key[:12], scopes, uuid.New())
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO api_key_index(key_hash, key_id, organization_id, project_id, scopes) VALUES($1,$2,$3,$4,$5)",
		hash[:], keyID, id.org, id.project, scopes)
	require.NoError(t, err)
	return id
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func waitReady(t *testing.T, endpoint string) {
	t.Helper()
	client := &http.Client{Timeout: 300 * time.Millisecond}
	defer client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, http.NoBody)
		if err != nil {
			return false
		}
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond)
}
