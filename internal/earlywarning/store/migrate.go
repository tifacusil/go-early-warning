package store

import (
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// migrationsFS embeds the versioned schema so the binary is self-sufficient:
// no external files needed to migrate (CLAUDE.md §2.4).
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies all pending up-migrations to the store's database. It is
// idempotent: golang-migrate tracks the applied version in schema_migrations
// and skips migrations already run, so it is safe to call on every startup.
func (s *Store) Migrate() error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrate source: %w", err)
	}
	driver, err := mysql.WithInstance(s.db, &mysql.Config{})
	if err != nil {
		return fmt.Errorf("migrate driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "mysql", driver)
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
