//go:build integration

// Package testdb starts the pinned TimescaleDB with production-shaped roles.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/pkg/testutil/pgtest"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/migrations"
)

const image = "timescale/timescaledb:2.32.0-pg17@sha256:6a6b7d7a346d7c6be03a310c240706f3bbc6d7adbdf3fbc07823570b34337f5e"
const readerRole = "svc_reader"

type DB struct {
	App, Reader, Batch *postgres.Pool
	MigratorURL        string
}

func Start(t *testing.T) DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Second)
	defer cancel()
	ctr, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase(pgtest.Database), tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword(secret(t)),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(time.Minute)))
	if ctr != nil {
		testcontainers.CleanupContainer(t, ctr)
	}
	require.NoError(t, err)
	superURL, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	conn, err := pgx.Connect(ctx, superURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "SET log_min_error_statement = panic")
	require.NoError(t, err)
	urls := make(map[string]string, 4)
	for _, role := range []string{pgtest.AppRole, pgtest.BatchRole, pgtest.MigratorRole, readerRole} {
		pw := secret(t)
		access := "NOBYPASSRLS"
		if role == pgtest.BatchRole {
			access = "BYPASSRLS"
		}
		_, execErr := conn.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN "+access+" PASSWORD '"+pw+"'")
		require.NoError(t, execErr)
		parsed, parseErr := url.Parse(superURL)
		require.NoError(t, parseErr)
		parsed.User = url.UserPassword(role, pw)
		urls[role] = parsed.String()
	}
	provisioning := pgtest.ProvisioningSQL(pgtest.Database, pgtest.MigratorRole, pgtest.AppRole, pgtest.BatchRole)
	provisioning = append(provisioning, pgtest.SchemaSQL(pgtest.MigratorRole, pgtest.AppRole, pgtest.BatchRole)...)
	provisioning = append(provisioning, "GRANT CONNECT ON DATABASE svc TO svc_reader", "GRANT USAGE ON SCHEMA public TO svc_reader")
	for _, statement := range provisioning {
		_, execErr := conn.Exec(ctx, statement)
		require.NoError(t, execErr)
	}
	logger := slog.New(slog.DiscardHandler)
	require.NoError(t, migrations.Migrate(ctx, urls[pgtest.MigratorRole], logger))
	pgtest.AssertTenantSchema(t, urls[pgtest.MigratorRole], pgtest.AppRole, []string{"goose_db_version", "signals_outbox"}, nil)
	return DB{App: open(t, urls[pgtest.AppRole]), Reader: open(t, urls[readerRole]), Batch: open(t, urls[pgtest.BatchRole]),
		MigratorURL: urls[pgtest.MigratorRole]}
}

func open(t *testing.T, dsn string) *postgres.Pool {
	t.Helper()
	pool, err := postgres.Open(t.Context(), postgres.Config{URL: dsn, ApplicationName: "stream-integration", MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Shutdown(ctx))
	})
	return pool
}

func secret(t *testing.T) string {
	t.Helper()
	var random [16]byte
	_, err := rand.Read(random[:])
	require.NoError(t, err)
	return hex.EncodeToString(random[:])
}
