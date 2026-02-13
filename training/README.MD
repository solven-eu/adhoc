Welcome in the training module for Adhoc

# What's Adhoc

Adhoc enables Spreadsheet-like Formulas over OLAP.

# How do I load data into Adhoc?

You do not load data into Adhoc. Adhoc will query underlying databases (leaving the aggregation work to them), and apply your formulas.

# How to I query Adhoc?

Before querying with Adhoc, you need to define a Cube through an `ICubeWrapper`:
1. To configure an `ITableWrapper`, representing the connection to an external Database, operating as an aggregation engine (e.g. doing `GROUP BY` SQL queries).
2. To configure your measure into an `IMeasureForest`. Each measure is a formula over underlying measures. Root measures are aggregations executed by the `ITableWrapper`
3. To wire the `ITableWrapper` and the `IMeasureForest` into an `ICubeWrapper`. This step enables additional customization (e.g. `IImplicitFilter`, `ICubeQueryEngine`, `IColumnsManager`, etc).

A Cube is a multi-dimentional object:
- each dimension can be used to decompose (i.e. `GROUP BY`) measures. Most dimension are equivalent to a column of the underlying table.
- each dimension can be used to filter (i.e. `WHERE`). Filtering is configured through `ISliceFilter` instances.

A query will:
- filter a section of the cube (i.e. `WHERE`) given a `ISliceFilter`.
- decompose this section in slices given the `GROUP BY` defined by an `IAdhocGroupBy`.
- return a value for each selected measure, for each slice.

A slice is:
- a section of the cube
- expressing a set of columns (from none to any number of columns)
- each column being associated to a single coordinate
- for instance: `country=FR`, or `country=FR&city=Paris`

# How to I define complex measures in Adhoc?

In Adhoc, measures are formulas applying one transformator over underlying measures:
- The root measures are aggregations (i.e. `IAggregation`). The most common aggregation is the sum (`SumAggregation`). You can define your own `IAggregation`.
- `Combinator` is the simplest transformator: it applies an `ICombination` on a per-slice basis.
- `Partitionor` is mandatory in any non-trivial peoject: it evaluates underlying measures with an additional `GROUP BY`, applying an `ICombination` on each partition, and aggregating the results up to the requested slice.
  - It enables use-cases like ForeignExchange (i.e. partitioning by currency then converting each partition to the same currency)
- `Shiftor` enables manipulating the requested slice.
  - It enables forcing the measure to be evaluated into a different coordinate. e.g. evaluating a measure for the previous date along some `AsOf` column.
- `Filtrator` enforces some additional filters.
  - It enables filtering the measure into some coordinate. e.g. forcing a measure to be evaluated for `country=FR`
- `Unfiltrator` enforces the removal of some filters.
  - It enables the measure to be evaluated in a wider range. e.g. forcing a measure not to have any filter along `country`.
- `Dispatchor` is the most complex transformator.
  - The range of application is very wide, as it generally catch use-cases not covered by previous building blocks.
  - One typical case is the `Manyy2Many`: one may want to build a dynamic column, where each member should behave as the union of a given set of members along another column. (e.g. `country_group=G8` representing `country=in=FR,US,UK`)