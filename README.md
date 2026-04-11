# pgxcursor

[![CI](https://github.com/pgx-contrib/pgxcursor/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxcursor/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxcursor)](https://github.com/pgx-contrib/pgxcursor/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxcursor.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxcursor)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxcursor)](./LICENSE)

cursor-based row iterator for [pgx v5](https://github.com/jackc/pgx).

## Features

- Drop-in `pgx.Rows` implementation — works with `pgx.RowToStructByName` and friends
- Configurable `Capacity` for batch-fetching (`FETCH N`) or single-row (`FETCH NEXT`)
- Automatic transaction management — cursor lifecycle is handled internally
- Works with `*pgxpool.Pool`, `*pgx.Conn`, or any `pgx.Tx`

## Installation

```bash
go get github.com/pgx-contrib/pgxcursor
```

## Usage

```go
package main

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/pgx-contrib/pgxcursor"
)

type User struct {
    ID   int    `db:"id"`
    Name string `db:"name"`
}

func main() {
    pool, err := pgxpool.New(context.Background(), "postgres://localhost/mydb")
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    querier := &pgxcursor.Querier{
        Querier:  pool,
        Capacity: 100, // fetch 100 rows per round-trip
    }

    rows, err := querier.Query(context.Background(), "SELECT id, name FROM users ORDER BY id")
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        user, err := pgx.RowToStructByName[User](rows)
        if err != nil {
            panic(err)
        }
        fmt.Println(user.Name)
    }

    if err := rows.Err(); err != nil {
        panic(err)
    }
}
```

## Capacity strategy

| `Capacity`    | SQL issued per batch     | Best for                                |
| ------------- | ------------------------ | --------------------------------------- |
| `0` (default) | `FETCH NEXT FROM cursor` | Low-memory, one row at a time           |
| `N > 0`       | `FETCH N FROM cursor`    | Throughput — tune N to your working set |

## Development

### DevContainer

Open in VS Code with the Dev Containers extension. The environment provides Go,
PostgreSQL 18, and Nix automatically.

```
PGX_DATABASE_URL=postgres://vscode@postgres:5432/pgxcursor?sslmode=disable
```

### Nix

```bash
nix develop          # enter shell with Go
go tool ginkgo run -r
```

### Run tests

```bash
# Unit tests only (no database required)
go tool ginkgo run -r

# With integration tests
export PGX_DATABASE_URL="postgres://localhost/pgxcursor?sslmode=disable"
go tool ginkgo run -r
```

## License

[MIT](./LICENSE).
