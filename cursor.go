package pgxcursor

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Queryable is the interface that wraps the Query method.
type Queryable interface {
	// Begin starts a pseudo nested transaction.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Exec executes a query that doesn't return rows.
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	// Query executes a query that returns rows.
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

// Querier represents a PostgreSQL cursor querier.
type Querier struct {
	// Capacity is the maximum number of rows to fetch for each iteration.
	Capacity int
	// Querier is the interface that wraps the Query method.
	Querier Queryable
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

	query = fmt.Sprintf("DECLARE %q CURSOR FOR %s", name, query)
	// declare the cursor
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		// rollback the transaction
		tx.Rollback(ctx)
		// return the error
		return nil, err
	}

	// prepare the cursor
	cursor := &Rows{
		tx:   tx,
		ctx:  ctx,
		cap:  c.Capacity,
		name: name,
	}

	return cursor, nil
}

var _ pgx.Rows = &Rows{}

// Rows is a wrapper around pgx.Rows.
type Rows struct {
	cap  int
	err  error
	name string
	tx   pgx.Tx
	rows pgx.Rows
	ctx  context.Context
}

// Err implements pgx.Rows.
func (r *Rows) Err() error {
	if r.rows != nil {
		return r.rows.Err()
	}
	return r.err
}

// Conn implements pgx.Rows.
func (r *Rows) Conn() *pgx.Conn {
	return r.tx.Conn()
}

// Close implements pgx.Rows.
func (r *Rows) Close() {
	if r.rows != nil {
		// close the rows
		r.close()
	}
	// rollback the transaction
	if err := r.tx.Rollback(r.ctx); err != nil {
		r.err = err
	}
}

// FieldDescriptions implements pgx.Rows.
func (r *Rows) FieldDescriptions() []pgconn.FieldDescription {
	if r.rows != nil {
		return r.rows.FieldDescriptions()
	}
	// noop
	return nil
}

// CommandTag implements pgx.Rows.
func (r *Rows) CommandTag() pgconn.CommandTag {
	if r.rows != nil {
		return r.rows.CommandTag()
	}
	// noop
	return pgconn.CommandTag{}
}

// Next implements pgx.Rows.
func (r *Rows) Next() bool {
	if r.rows == nil {
		// move the cursor
		return r.next()
	}

	if !r.rows.Next() {
		// close the rows
		r.close()
		// move to the next row
		return r.next()
	}

	return true
}

// Scan implements pgx.Rows.
func (r *Rows) Scan(dest ...any) error {
	if r.rows != nil {
		return r.rows.Scan(dest...)
	}
	// noop
	return nil
}

// RawValues implements pgx.Rows.
func (r *Rows) RawValues() [][]byte {
	if r.rows != nil {
		return r.rows.RawValues()
	}
	// noop
	return nil
}

// Values implements pgx.Rows.
func (r *Rows) Values() ([]any, error) {
	if r.rows != nil {
		return r.rows.Values()
	}
	// noop
	return nil, nil
}

// next fetches the next rows.
func (r *Rows) next() bool {
	var query string
	// prepare the query
	if r.cap > 0 {
		query = fmt.Sprintf("FETCH %d FROM %v", r.cap, r.name)
	} else {
		query = fmt.Sprintf("FETCH NEXT FROM %v", r.name)
	}
	// if name is empty, then the cursor is not declared
	if r.rows, r.err = r.tx.Query(r.ctx, query); r.err != nil {
		return false
	}

	return r.rows.Next()
}

// close closes the rows and sets the error if any.
func (r *Rows) close() {
	// close the rows
	r.rows.Close()
	// set the error if any
	r.err = r.rows.Err()
	// reset the rows
	r.rows = nil
}
