// Package payment exposes build-time assets embedded from the module root.
package payment

import "embed"

//go:embed migrations/*.sql
var MigrationsFS embed.FS
