# Lexicon

## Table

A `table` refers to database, where data is going on a per-column basis, with aggregation capabilities. The actual interface for tables is `ITableWrapper`.

Example tables are:
- `InMemoryTable` which is essential a `List` of `Map`s.
- `JooqTableWrapper` which can wraps many SQL-engine (e.g. `DuckDB`, `PostgreSQL`, `RedShift`, etc) with the help of JooQ

A `table` can run `TableQueries`.

## Column

## Measure

A `measure` is an indicator. *TODO Migrate from README.MD*

## Forest

A `forest` is a bunch of measures.  *TODO Migrate from README.MD*

## Cube

A `cube` is essentially the pairing of a `table` with a `forest`. It enables running `IAdhocQueries`.
