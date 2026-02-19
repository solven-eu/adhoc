# Lexicon

## OLAP

Online Analytical Processing.

It refers to running read-only queries, typically synthesizing a large quantity of data into a smaller set of data (e.g. with `GROUP BY`).

## Cube

A cube is a multidimensional structure. A cube can by drilled-down by:
- filtering, i.e. selecting a subset of data.
- decomposing, i.e. splitting
the requested measures ()

In `Adhoc`, a `ICubeWrapper` is essentially the pairing of a `table` with a `forest`.

## Table

A `table` refers to database, where data is going on a per-column basis, with aggregation capabilities.

In `Adhoc`, a `ITableWrapper` is the abstraction to external databases.

Example tables are:
- `InMemoryTable` which is essential a `List` of `Map`s.
- `JooqTableWrapper` which can wraps many SQL-engine (e.g. `DuckDB`, `PostgreSQL`, `RedShift`, etc) with the help of JooQ.
- `MongoTableWrapper` which demonstrate how to manage NoSQL databases.

A `table` can run `TableQueries`.

## Column

A column generally refers to the column of a table, or a hierarchy of a cube.

In the context of a cube, it may also be called with various names:
- dimensions
- hierarchies
- levels
- axes

A column may be scalar/primitive if it is materialized by an actual table column, or calculated (see `ICalculatedColumn`) when it is derived (e.g. from other columns).

## Measure

A `measure` is an indicator. One may be confused between:
- a transformator archetype (e.g. `Combinator`, `Partitionor`), defining the type of formula, which is essentially defined by its `ATransformatorQueryStep` (e.g. `CombinatorQueryStep`).
- a transformator (e.g. `SumCombination implements ICombination`), defining the formula but lacking its configuration.
- a measure (e.g. `Combinator.builder().name("measureName").combinationKey("someKey").build();`), defining an instance of a transformator (e.g. with a unique name).

## Forest

A `forest` is a tree of measures.
