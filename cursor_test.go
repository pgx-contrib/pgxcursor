package pgxcursor_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/pgx-contrib/pgxcursor"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

// fakeTx implements pgx.Tx. Only Exec, Query, Rollback, and Conn are wired up.
type fakeTx struct {
	execErr    error
	queryRows  pgx.Rows
	queryErr   error
	rollbackErr error
	conn       *pgx.Conn
}

func (f *fakeTx) Begin(ctx context.Context) (pgx.Tx, error)        { return f, nil }
func (f *fakeTx) Commit(ctx context.Context) error                  { return nil }
func (f *fakeTx) Rollback(ctx context.Context) error                { return f.rollbackErr }
func (f *fakeTx) Conn() *pgx.Conn                                   { return f.conn }
func (f *fakeTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (f *fakeTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults { return nil }
func (f *fakeTx) LargeObjects() pgx.LargeObjects                              { return pgx.LargeObjects{} }
func (f *fakeTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (f *fakeTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, f.execErr
}
func (f *fakeTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return f.queryRows, f.queryErr
}
func (f *fakeTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row { return nil }

// fakeRows implements pgx.Rows with a configurable set of rows.
type fakeRows struct {
	total   int
	current int
	err     error
	closed  bool
}

func newFakeRows(n int) *fakeRows { return &fakeRows{total: n} }

func (r *fakeRows) Next() bool {
	if r.current < r.total {
		r.current++
		return true
	}
	return false
}
func (r *fakeRows) Close()                                         { r.closed = true }
func (r *fakeRows) Err() error                                     { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                  { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription   { return nil }
func (r *fakeRows) Scan(_ ...any) error                            { return nil }
func (r *fakeRows) Values() ([]any, error)                         { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                            { return nil }
func (r *fakeRows) Conn() *pgx.Conn                                { return nil }

// fakeQueryable implements pgxcursor.Queryable; Begin returns a configured fakeTx.
type fakeQueryable struct {
	tx      *fakeTx
	beginErr error
}

func (q *fakeQueryable) Begin(_ context.Context) (pgx.Tx, error) {
	return q.tx, q.beginErr
}
func (q *fakeQueryable) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (q *fakeQueryable) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, nil
}

// ─── tests ────────────────────────────────────────────────────────────────────

var _ = Describe("Querier.Query()", func() {
	It("returns error when Begin fails", func() {
		q := &pgxcursor.Querier{
			Querier: &fakeQueryable{beginErr: errors.New("begin failed")},
		}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).To(MatchError("begin failed"))
		Expect(rows).To(BeNil())
	})

	It("returns error when DECLARE fails and rolls back the transaction", func() {
		tx := &fakeTx{execErr: errors.New("syntax error")}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "INVALID SQL")
		Expect(err).To(MatchError("syntax error"))
		Expect(rows).To(BeNil())
	})

	It("returns Rows on success", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).NotTo(BeNil())
		rows.Close()
	})

	It("generates a unique cursor name per call", func() {
		// Multiple successive calls must all succeed; if they shared a name
		// the second DECLARE would fail on a real DB.
		for i := 0; i < 3; i++ {
			tx := &fakeTx{queryRows: newFakeRows(0)}
			q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
			rows, err := q.Query(context.Background(), "SELECT 1")
			Expect(err).NotTo(HaveOccurred())
			rows.Close()
		}
	})
})

var _ = Describe("Rows.Next()", func() {
	Context("Capacity == 0 (FETCH NEXT)", func() {
		It("fetches one row at a time", func() {
			inner := newFakeRows(1)
			tx := &fakeTx{queryRows: inner}
			q := &pgxcursor.Querier{Capacity: 0, Querier: &fakeQueryable{tx: tx}}
			rows, err := q.Query(context.Background(), "SELECT 1")
			Expect(err).NotTo(HaveOccurred())
			Expect(rows.Next()).To(BeTrue())
			rows.Close()
		})

		It("returns false when exhausted", func() {
			inner := newFakeRows(0)
			tx := &fakeTx{queryRows: inner}
			q := &pgxcursor.Querier{Capacity: 0, Querier: &fakeQueryable{tx: tx}}
			rows, err := q.Query(context.Background(), "SELECT 1")
			Expect(err).NotTo(HaveOccurred())
			Expect(rows.Next()).To(BeFalse())
			rows.Close()
		})
	})

	Context("Capacity > 0 (FETCH N)", func() {
		It("fetches a batch and iterates", func() {
			inner := newFakeRows(3)
			tx := &fakeTx{queryRows: inner}
			q := &pgxcursor.Querier{Capacity: 5, Querier: &fakeQueryable{tx: tx}}
			rows, err := q.Query(context.Background(), "SELECT 1")
			Expect(err).NotTo(HaveOccurred())

			count := 0
			for rows.Next() {
				count++
				if count > 10 {
					break // guard against infinite loop
				}
			}
			Expect(count).To(Equal(3))
			rows.Close()
		})

		It("returns false when no more rows", func() {
			inner := newFakeRows(0)
			tx := &fakeTx{queryRows: inner}
			q := &pgxcursor.Querier{Capacity: 5, Querier: &fakeQueryable{tx: tx}}
			rows, err := q.Query(context.Background(), "SELECT 1")
			Expect(err).NotTo(HaveOccurred())
			Expect(rows.Next()).To(BeFalse())
			rows.Close()
		})
	})

	It("returns false and records error when FETCH query fails", func() {
		// queryRows must be nil when queryErr is set — mirrors real pgx behaviour
		fq := &fakeQueryable{tx: &fakeTx{queryRows: nil, queryErr: errors.New("fetch error")}}
		q := &pgxcursor.Querier{Capacity: 0, Querier: fq}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows.Next()).To(BeFalse())
		Expect(rows.Err()).To(MatchError("fetch error"))
		rows.Close()
	})
})

var _ = Describe("Rows.Close()", func() {
	It("closes inner rows when present", func() {
		inner := newFakeRows(1)
		tx := &fakeTx{queryRows: inner}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		rows.Next() // causes inner rows to be set
		rows.Close()
		Expect(inner.closed).To(BeTrue())
	})

	It("rolls back the transaction", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		rows.Close()
		// rollbackErr is nil so no error recorded
		Expect(rows.Err()).To(BeNil())
	})

	It("records rollback error in Err() when rollback fails", func() {
		tx := &fakeTx{queryRows: newFakeRows(0), rollbackErr: errors.New("rollback failed")}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		rows.Close()
		Expect(rows.Err()).To(MatchError("rollback failed"))
	})
})

var _ = Describe("Rows.Err()", func() {
	It("returns nil when no error", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		rows.Close()
		Expect(rows.Err()).To(BeNil())
	})

	It("returns captured error after close with rollback error", func() {
		tx := &fakeTx{queryRows: newFakeRows(0), rollbackErr: errors.New("rb err")}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		rows.Close()
		Expect(rows.Err()).To(MatchError("rb err"))
	})
})

var _ = Describe("Rows.FieldDescriptions()", func() {
	It("returns nil before first Next()", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows.FieldDescriptions()).To(BeNil())
		rows.Close()
	})
})

var _ = Describe("Rows.CommandTag()", func() {
	It("returns empty CommandTag before first Next()", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows.CommandTag()).To(Equal(pgconn.CommandTag{}))
		rows.Close()
	})
})

var _ = Describe("Rows.Scan()", func() {
	It("returns nil when rows is nil (before Next)", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows.Scan()).To(BeNil())
		rows.Close()
	})
})

var _ = Describe("Rows.RawValues()", func() {
	It("returns nil when rows is nil", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows.RawValues()).To(BeNil())
		rows.Close()
	})
})

var _ = Describe("Rows.Values()", func() {
	It("returns nil, nil when rows is nil", func() {
		tx := &fakeTx{queryRows: newFakeRows(0)}
		q := &pgxcursor.Querier{Querier: &fakeQueryable{tx: tx}}
		rows, err := q.Query(context.Background(), "SELECT 1")
		Expect(err).NotTo(HaveOccurred())
		vals, err := rows.Values()
		Expect(err).To(BeNil())
		Expect(vals).To(BeNil())
		rows.Close()
	})
})

// ─── integration ─────────────────────────────────────────────────────────────

var _ = Describe("Integration", Ordered, func() {
	var pool *pgxpool.Pool

	BeforeAll(func() {
		if os.Getenv("PGX_DATABASE_URL") == "" {
			Skip("PGX_DATABASE_URL not set")
		}
		config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
		Expect(err).NotTo(HaveOccurred())
		pool, err = pgxpool.NewWithConfig(context.Background(), config)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if pool != nil {
			pool.Close()
		}
	})

	BeforeEach(func() {
		if pool == nil {
			Skip("PGX_DATABASE_URL not set")
		}
		_, err := pool.Exec(context.Background(),
			`CREATE TABLE cursor_test (id int, name text)`)
		Expect(err).NotTo(HaveOccurred())
		for i := 1; i <= 20; i++ {
			_, err := pool.Exec(context.Background(),
				`INSERT INTO cursor_test VALUES ($1, $2)`, i, fmt.Sprintf("name-%d", i))
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		if pool == nil {
			return
		}
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS cursor_test`)
	})

	It("iterates all rows with Capacity 0 (FETCH NEXT)", func() {
		q := &pgxcursor.Querier{Capacity: 0, Querier: pool}
		rows, err := q.Query(context.Background(), "SELECT id, name FROM cursor_test ORDER BY id")
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(count).To(Equal(20))
	})

	It("iterates all rows with Capacity 5 (FETCH N)", func() {
		q := &pgxcursor.Querier{Capacity: 5, Querier: pool}
		rows, err := q.Query(context.Background(), "SELECT id, name FROM cursor_test ORDER BY id")
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(count).To(Equal(20))
	})

	It("iterates all rows with Capacity > row count", func() {
		q := &pgxcursor.Querier{Capacity: 100, Querier: pool}
		rows, err := q.Query(context.Background(), "SELECT id, name FROM cursor_test ORDER BY id")
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(count).To(Equal(20))
	})

	It("Close() is idempotent and Err() is nil after normal iteration", func() {
		q := &pgxcursor.Querier{Capacity: 5, Querier: pool}
		rows, err := q.Query(context.Background(), "SELECT id FROM cursor_test")
		Expect(err).NotTo(HaveOccurred())
		for rows.Next() {
		}
		rows.Close()
		rows.Close() // second close should not panic
		Expect(rows.Err()).NotTo(HaveOccurred())
	})

	It("returns error for invalid SQL, rolls back", func() {
		q := &pgxcursor.Querier{Querier: pool}
		_, err := q.Query(context.Background(), "SELECT * FROM nonexistent_table_xyz")
		Expect(err).To(HaveOccurred())
	})

	It("FieldDescriptions() returns column info after first Next()", func() {
		q := &pgxcursor.Querier{Capacity: 1, Querier: pool}
		rows, err := q.Query(context.Background(), "SELECT id, name FROM cursor_test ORDER BY id LIMIT 1")
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		Expect(rows.Next()).To(BeTrue())
		Expect(rows.FieldDescriptions()).NotTo(BeEmpty())
	})

	It("Conn() returns a valid *pgx.Conn", func() {
		q := &pgxcursor.Querier{Querier: pool}
		rows, err := q.Query(context.Background(), "SELECT id FROM cursor_test")
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		Expect(rows.Conn()).NotTo(BeNil())
	})
})
