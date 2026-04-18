# FAQ — Recurrent cases

A non-exhaustive list of common questions and one (or more) solutions. This page is meant to grow
over time: when a question comes up repeatedly, add it here with a short, actionable answer and
pointers to the relevant reference pages.

## Columns

### How can I manage a column used by a measure, but missing in the database?

When a measure (or an aggregator) references a column that does not exist in the underlying
`ITableWrapper`, the table query will fail because Adhoc asks for a column the database does not
know about.

**Simple solution — register a `FunctionCalculatedColumn` with a constant value on the
`ColumnsManager`.**

```java
FunctionCalculatedColumn missingAsConstant = FunctionCalculatedColumn.builder()
		.name("missingColumn")
		.type(String.class)
		.recordToCoordinate(record -> "defaultValue")
		.build();

ColumnsManager columnsManager = ColumnsManager.builder()
		.calculatedColumn(missingAsConstant)
		.build();
```

The calculated column is evaluated per-row right after the `ITableWrapper` returns, so every row
looks as if the table had a `missingColumn` column holding `"defaultValue"`. Measures, filters and
GROUP BYs then see the column like any other.

**Alternative — register an `IColumnGenerator` manually on the `ColumnsManager`.**

`ColumnsManager.builder().columnGenerator(...)` accepts a single `IColumnGenerator`. Unlike
`ICalculatedColumn`, an `IColumnGenerator` advertises a set of column names and types through
`getColumnTypes()` without being wired to a specific `Dispatchor` measure; use this when the
missing column must be reported in the cube schema (`cube.getColumns()`) even though no
per-record evaluation logic is required. The two mechanisms are redundant today and are
expected to converge — see the TODO near `ColumnsManager.columnGenerator` / `calculatedColumns`.

**When to pick another approach:**

- If the value must depend on other columns of the row, use a regular `FunctionCalculatedColumn`
  that reads from the `ITabularGroupByRecord` — see [Calculated Columns](calculated-columns.md).
- If the value should be authored as a string expression (e.g. by a UI user), use
  `EvaluatedExpressionColumn` instead.
- If several rows must be produced from one (EXPLODE semantics), use an `IDecomposition` via a
  `Dispatchor` — see [Many-to-many](many-to-many.md).

See [Calculated Columns](calculated-columns.md) for the full reference.

## See also

- [Calculated Columns](calculated-columns.md)
- [Tables](tables.md)
- [Debug / Investigations](debug.md)

