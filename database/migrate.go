package database

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// RunMigrations applies pending .up.sql files in lexicographic order.
// Each migration runs in a transaction — if it fails, nothing is committed.
// Already-applied versions are tracked in the schema_migrations table.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, migrationsPath string, logger zerolog.Logger) error {
	// Bootstrap the tracking table on first run.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		return err
	}

	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		return err
	}

	// Collect only "up" migrations and sort to guarantee order.
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)

	for _, file := range files {
		version := strings.TrimSuffix(file, ".up.sql")

		// Skip if already applied.
		var applied bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}

		sql, err := os.ReadFile(filepath.Join(migrationsPath, file))
		if err != nil {
			return err
		}

		// Wrap migration + version insert in a single transaction for atomicity.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}

		logger.Info().Str("migration", version).Msg("applied migration")
	}

	return nil
}
