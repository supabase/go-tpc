# Go TPC

A command line tool to run [TPC](http://www.tpc.org/)-like workloads.

This is a fork of https://github.com/pingcap/go-tpc containing several improvements over the original code base:

* Better compatibility with the TPC-C spec
* Additional features such as structured CSV and JSON output
* Stricter error handling
* Up-to-date dependencies

and many more.

We support the following workloads:

* TPC-C
* TPC-H
* [CH-benCHmark](https://db.in.tum.de/research/projects/CHbenCHmark/)

and the following databases:

* Postgres and compatible databases such as CockroachDB, AlloyDB or Yugabyte
* MySQL and compatible databases such as TiDB

Our primary target with the most complete coverage for practical workloads is the TPC-C workload for Postgres but we strive to keep feature parity across all supported workloads and database systems.

## Install

Use one of the following approaches:

### Install script (recommended)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/supabase/go-tpc/master/install.sh | sh
```

### Download binary

```bash
 curl -LO "https://github.com/supabase/go-tpc/releases/download/latest/go-tpc_latest_$(uname -s | tr 'A-Z' 'a-z')_$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/').tar.gz"
 tar -xzf go-tpc_latest_*.tar.gz
```

We recommend you put `go-tpc` on `$PATH`.

### Build from source

```bash
git clone https://github.com/supabase/go-tpc.git
cd go-tpc
make build
```

The `go-tpc` binary is built in `./bin`.

## Usage

By default, go-tpc uses `root::@tcp(127.0.0.1:4000)/test` as the default dsn address, you can override it by setting below flags:

```bash
  -D, --db string           Database name (default "test")
  -H, --host string         Database host (default "127.0.0.1")
  -p, --password string     Database password
  -P, --port int            Database port (default 4000)
  -U, --user string         Database user (default "root")

```

> **Note:**
>
> When exporting csv files to a directory, `go-tpc` will also create the necessary tables for further data input if
> the provided database address is accessible.

For example:

```bash
./bin/go-tpc -H 127.0.0.1 -P 3306 -D tpcc ...
```

### TPC-C

#### Prepare

##### Postgres

```bash
go-tpc tpcc prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable

# Refresh optimizer statistics after loading (VACUUM ANALYZE on postgres, ANALYZE TABLE on mysql). 
# Only enable if the server does this maintenance automatically during the run, i.e. autovacuum 
# on Postgres or innodb_stats_auto_recalc on MySQL.
go-tpc tpcc prepare --analyze -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

##### MySQL

```bash
# Create 4 warehouses with 4 threads
go-tpc tpcc --warehouses 4 prepare -T 4
```

#### Run

##### Postgres

```bash
go-tpc tpcc run -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

##### MySQL

```bash
# Run TPCC workloads, you can just run or add --wait option to including wait times
go-tpc tpcc --warehouses 4 run -T 4
# Run TPCC including wait times(keying & thinking time) on every transactions
go-tpc tpcc --warehouses 4 run -T 4 --wait
```

#### Check

```bash
# Check consistency. you can check after prepare or after run
go-tpc tpcc --warehouses 4 check
```

#### Clean up

```bash
# Cleanup
go-tpc tpcc --warehouses 4 cleanup
```

#### Other usages

```bash
# Generate csv files (split to 100 files each table)
go-tpc tpcc --warehouses 4 prepare -T 100 --output-type csv --output-dir data
# Specified tables when generating csv files
go-tpc tpcc --warehouses 4 prepare -T 100 --output-type csv --output-dir data --tables history,orders
# Start pprof
go-tpc tpcc --warehouses 4 prepare --output-type csv --output-dir data --pprof :10111
```

### TPC-H

#### Prepare

##### Postgres

```bash
go-tpc tpch prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

##### MySQL

```bash
# Prepare data with scale factor 1
go-tpc tpch --sf=1 prepare
# Prepare data with scale factor 1, create tiflash replica, and analyze table after data loaded
go-tpc tpch --sf 1 --analyze --tiflash-replica 1 prepare
```

#### Run

##### Postgres

```bash
go-tpc tpch run -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

##### MySQL

```bash
# Run TPCH workloads with result checking
go-tpc tpch --sf=1 --check=true run
# Run TPCH workloads without result checking
go-tpc tpch --sf=1 run
```

#### Clean up

```bash
# Cleanup
go-tpc tpch cleanup
```

### CH-benCHmark

CH-benCHmark is a hybrid benchmark that runs a transactional and an analytical workload.

#### Prepare

The preparation is a two-step process:

1. Prepare the schema and data for the transactional part with `go-tpc tpcc --warehouses $warehouses prepare`.
2. Prepare the schema and data for the analytical part with `go-tpc ch prepare`

##### Postgres

``` bash
# Prepare TP data
go-tpc tpcc prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable -T 4
# Prepare AP data
go-tpc ch prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

##### MySQL
```bash
# Prepare TP data
go-tpc tpcc --warehouses 10 prepare -T 4 -D test -H 127.0.0.1 -P 4000
# Prepare AP data, create tiflash replica, and analyze table after data loaded
go-tpc ch --analyze --tiflash-replica 1 prepare -D test -H 127.0.0.1 -P 4000
```

#### Run

##### Postgres

```bash
go-tpc ch run -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

##### MySQL
```bash
go-tpc ch --warehouses $warehouses -T $tpWorkers -t $apWorkers --time $measurement-time run
```

### Raw SQL

The `rawsql` command is used to execute SQL from the provided SQL files.

#### Run
```bash
go-tpc rawsql run --query-files $path-to-query-files
```
