package pgxiter_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgx-contrib/pgxiter"
)

func ExampleQuerier() {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	pool, err := pgxpool.NewWithConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}
	// close the pool
	defer pool.Close()

	iterator := &pgxiter.Querier{Querier: pool}
	// start the cursor
	rows, err := iterator.Query(context.TODO(), "SELECT * FROM user")
	if err != nil {
		panic(err)
	}
	// close the cursor
	defer rows.Close()

	// User represents a user.
	type User struct {
		Name     string `db:"name"`
		Password string `db:"password"`
	}

	for rows.Next() {
		user, err := pgx.RowToStructByName[User](rows)
		if err != nil {
			panic(err)
		}

		fmt.Println(user.Name)
	}
}
