# Calculated Columns

"Calculated column" is an umbrella term for several mechanisms that produce columns not present in
the underlying table. This page disambiguates the three approaches Adhoc offers, their trade-offs,
and when to pick each.

## Overview

|                     Approach                     |                    Interface                     |                              Where it runs                               |                          Input                          |              Output               |                                Typical use                                |
|--------------------------------------------------|--------------------------------------------------|--------------------------------------------------------------------------|---------------------------------------------------------|-----------------------------------|---------------------------------------------------------------------------|
| **Column Generator** (measure-side)              | `IColumnGenerator` / `IDecomposition`            | Inside a `Dispatchor` measure, after the underlying measure is evaluated | A measure value + the current slice                     | One or more new columns (EXPLODE) | Decomposing a `Map` value, many-to-many, JOIN-as-measure                  |
| **Calculated Column** (table-side, programmatic) | `ICalculatedColumn` / `FunctionCalculatedColumn` | Right after the `ITableWrapper` returns rows, before aggregation         | The full `ITabularGroupByRecord` (groupBy + aggregates) | A single new column per row       | First letter of a country, date → quarter, any per-record `Function`      |
| **Calculated Column** (table-side, expression)   | `EvaluatedExpressionColumn`                      | Same as above                                                            | Same as above                                           | A single new column per row       | User-authored expressions via [EvalEx](https://github.com/ezylang/EvalEx) |

## 1. Column generators — `IDecomposition`

An `IDecomposition` (which extends `IColumnGenerator`) is wired into a `Dispatchor` measure. It
receives a value and a slice, and returns a list of `IDecompositionEntry`, each carrying new column
coordinates — conceptually an **EXPLODE** operation. If the generated column appears in the query's
GROUP BY, the decomposition's `getUnderlyingSteps` automatically adds the necessary input columns
to the underlying step.

Use this when the new column depends on a complex measure value (e.g. a `Map` whose keys become
column coordinates) or when the mapping comes from an external lookup table.

Examples:
- `ManyToMany1DDecomposition` — one input column maps to multiple output groups.
- `JoinDecomposition` — lookup-JOIN on a dimension table (see [Many-to-many](many-to-many.md)).
- `DuplicatingDecomposition` — copies the value into all target coordinates.

See [Many-to-many](many-to-many.md) and [Custom Measures](custom-measure.md) for full examples.

## 2. `FunctionCalculatedColumn` — programmatic per-record column

`FunctionCalculatedColumn` implements `ICalculatedColumn` and wraps a
`Function<ITabularGroupByRecord, Object>`. The engine evaluates it **right after** the
`ITableWrapper` returns rows, on a per-record basis, so the result is available for GROUP BY and
filtering like any native column.

```java
FunctionCalculatedColumn firstLetter = FunctionCalculatedColumn.builder()
		.name("firstLetter")
		.type(String.class)
		.recordToCoordinate(record -> {
			String country = (String) record.getGroupBy("country");
			return country.substring(0, 1);
		})
		.build();
```

### Automatic underlying-column propagation

`ColumnsManager.transcodeGroupBy(AliasingContext, IGroupBy)` detects `FunctionCalculatedColumn`
instances in the GROUP BY and automatically adds the columns the function reads (discovered via a
recording probe — see `FunctionCalculatedColumn.getUnderlyingColumns`). This means a query that
says `GROUP BY firstLetter` will internally also request `country` from the table, without the
caller having to specify it.

### Registration

A `FunctionCalculatedColumn` can be provided in two ways:

1. **Statically** — registered on a `ColumnsManager` (via `IColumnsManager.calculatedColumns`).
   Every query on the cube sees it.
2. **Dynamically** — embedded directly in the `IGroupBy` of a `CubeQuery`. The column lives only
   for that query.

## 3. `EvaluatedExpressionColumn` — user-authored expression

`EvaluatedExpressionColumn` also implements `ICalculatedColumn`. Instead of a `Function`, it
takes a `String` expression evaluated at runtime by [EvalEx](https://github.com/ezylang/EvalEx)
(optional dependency — see [SECURITY.MD § EvalEx](../SECURITY.MD#evalex--expression-evaluation-optional-dependency)).

```java
EvaluatedExpressionColumn quarter = EvaluatedExpressionColumn.builder()
		.name("quarter")
		.expression("FLOOR((month - 1) / 3) + 1")
		.build();
```

This is useful when the expression is authored by the **user** (e.g. through a UI) rather than
hardcoded. **Sanitise the expression** before evaluation if it comes from an untrusted source.

!!! warning "Known limitation"
`ColumnsManager.transcodeGroupBy` currently propagates underlying columns for
`FunctionCalculatedColumn` but **not** for `EvaluatedExpressionColumn`. This means a
`GROUP BY quarter` will not automatically add `month` to the table query. This looks like a
bug — it should be done for any `ICalculatedColumn`. Track the fix via the TODO in
`ColumnsManager.transcodeGroupBy`.

## Choosing the right approach

|                               Question                               |                     Answer                     |
|----------------------------------------------------------------------|------------------------------------------------|
| Do I need to EXPLODE a value into multiple coordinates?              | Use `IDecomposition` via a `Dispatchor`.       |
| Is the column a simple per-row function of existing columns?         | Use `FunctionCalculatedColumn`.                |
| Should the user be able to author the column definition dynamically? | Use `EvaluatedExpressionColumn`.               |
| Do I need a lookup-JOIN from an external table?                      | Use `JoinDecomposition` (an `IDecomposition`). |

## See also

- [ICombination](combination.md) — business logic that reads but does not generate columns.
- [Many-to-many](many-to-many.md) — the canonical `IDecomposition` use case.
- [Partitionor](partitionor.md) — GROUP BY widening, which often pairs with a calculated column.
- [Custom Measures](custom-measure.md) — arbitrary measure evaluation logic.
- [Tables](tables.md) — the `ITableWrapper` that calculated columns post-process.

