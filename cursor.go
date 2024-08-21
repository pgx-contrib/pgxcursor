package pgxiter

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Queriable is the interface that wraps the Query method.
type Queriable interface {
	// Begin starts a pseudo nested transaction.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Exec executes a query that doesn't return rows.
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	// Query executes a query that returns rows.
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

// Querier represents a PostgreSQL cursor querier.
type Querier struct {
	// Querier is the interface that wraps the Query method.
	Querier Queriable
}

// Query executes a query that returns rows.
func (c *Querier) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	// predefined cursor name
	name := fmt.Sprintf("c%x", uuid.New())

	// begin a transaction
	tx, err := c.Querier.Begin(ctx)
	if err != nil {
		return nil, err
	}

	// prepare the rows
	rows := &Cursor{
		// general information
		tx:  tx,
		ctx: ctx,
		// cursor information
		name: name,
	}

	query = fmt.Sprintf("DECLARE %q CURSOR FOR %s", rows.name, query)
	// declare the cursor
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		// rollback the transaction
		tx.Rollback(ctx)
		// return the error
		return nil, err
	}

	return rows, nil
}

var _ pgx.Rows = &Cursor{}

// Cursor is a wrapper around pgx.Cursor.
type Cursor struct {
	// general information
	tx  pgx.Tx
	err error
	ctx context.Context
	// cursor information
	name string
	rows pgx.Rows
}

// Err implements pgx.Rows.
func (r *Cursor) Err() error {
	if r.rows != nil {
		return r.rows.Err()
	}
	return r.err
}

// Conn implements pgx.Rows.
func (r *Cursor) Conn() *pgx.Conn {
	return r.tx.Conn()
}

// Close implements pgx.Rows.
func (r *Cursor) Close() {
	// close the rows
	if r.rows != nil {
		r.rows.Close()
		r.rows = nil
	}
	// rollback the transaction
	if err := r.tx.Rollback(r.ctx); err != nil {
		r.err = err
	}
}

// FieldDescriptions implements pgx.Rows.
func (r *Cursor) FieldDescriptions() []pgconn.FieldDescription {
	if r.rows != nil {
		return r.rows.FieldDescriptions()
	}
	// noop
	return nil
}

// CommandTag implements pgx.Rows.
func (r *Cursor) CommandTag() pgconn.CommandTag {
	if r.rows != nil {
		return r.rows.CommandTag()
	}
	// noop
	return pgconn.CommandTag{}
}

// Next implements pgx.Rows.
func (r *Cursor) Next() bool {
	if r.rows == nil {
		// declare the cursor
		query := "FETCH NEXT FROM " + r.name
		// if name is empty, then the cursor is not declared
		if r.rows, r.err = r.tx.Query(r.ctx, query); r.err != nil {
			return false
		}
	}

	if !r.rows.Next() {
		// close the rows
		r.rows.Close()
		r.rows = nil
		// done!
		return false
	}

	return true
}

// Scan implements pgx.Rows.
func (r *Cursor) Scan(dest ...any) error {
	if r.rows != nil {
		return r.rows.Scan(dest...)
	}
	// noop
	return nil
}

// RawValues implements pgx.Rows.
func (r *Cursor) RawValues() [][]byte {
	if r.rows != nil {
		return r.rows.RawValues()
	}
	// noop
	return nil
}

// Values implements pgx.Rows.
func (r *Cursor) Values() ([]any, error) {
	if r.rows != nil {
		return r.rows.Values()
	}
	// noop
	return nil, nil
}
