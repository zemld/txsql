package pgx

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

type row struct {
	source pgx.Row
}

func (r row) Scan(dest ...any) error {
	if err := r.source.Scan(dest...); err != nil {
		return fmt.Errorf("query row: %w", err)
	}

	return nil
}
