# Complex SQL with JooQ

Adhoc speaks SQL through [JooQ](https://www.jooq.org/). The simplest case — a single table — is
already covered in [Tables](tables.md): hand `JooqTableWrapperParameters` a `Table<?>` and you're
done. This article is about the next step up: snowflake / star schemas, multi-key joins, diamond
joins, join pruning, and the column-alias mechanism — everything `JooqTableSupplierBuilder` and
its pruning subclass `PrunedJoinsJooqTableSupplierBuilder` exist for.

## When you need it

Use the builder when any of the following is true:

- Your table is a **multi-arm snowflake** (fact joined to several dimensions, dimensions
  themselves joined to deeper dimensions). Hand-rolling the JooQ `FROM` clause becomes verbose
  and error-prone fast.
- You expose **caller-friendly column names** that don't match the underlying SQL columns —
  e.g. you want callers to query `country` even though the actual column is
  `customers.customer_country`.
- Most queries touch **only a subset** of the joins, and you want unused arms pruned out of
  the SQL `FROM` clause to reduce engine work (see [Join pruning](#join-pruning) below).

If none of those apply, the plain `JooqTableWrapperParameters.builder().table(DSL.table(...))`
path is enough — don't reach for the builder.

## Basic snowflake

```java
PrunedJoinsJooqTableSupplierBuilder schema = PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
		.baseTable(DSL.table("orders"))
		.baseTableAlias("f")
		.dslSupplier(dslSupplier)
		.build()
		// f → customers
		.leftJoin(j -> j.table(DSL.table("customers")).alias("c").on("customer_id", "id"))
		// f → products → categories  (snowflake leg: `.from("p")` switches the parent)
		.leftJoin(j -> j.table(DSL.table("products")).alias("p").on("product_id", "id"))
		.leftJoin(j -> j.table(DSL.table("categories"))
				.alias("cat")
				.from("p")
				.on("category_id", "id"));

JooqTableWrapperParameters params = JooqTableWrapperParameters.builder()
		.dslSupplier(dslSupplier)
		.tableSupplier(PrunedJoinsJooqTableSupplier.builder().schema(schema).build())
		.build();
ITableWrapper table = new JooqTableWrapper("orders", params);
```

The `tableSupplier(IJooqTableSupplier)` setter wires both `.table(...)` (used for schema
introspection — `getColumns`) **and** the per-query pruning supplier in one go. You no longer
have to hand `.table(...)` separately.

## Diamond joins

Adhoc inherits the **full SQL join expressiveness** — diamond joins, asOf joins, window-function
joins, multi-key composite ON-clauses are all supported. This is one of the major differences
with Atoti, where the schema is restricted to a single-parent star/snowflake tree.

A diamond join is one where the joined table depends on columns from **multiple parent tables**.
Classic shape: `pricing_rules` keyed by `(region_id, segment)`, where `region_id` lives on the
fact table and `segment` lives on the customers dimension.

```java
.leftJoin(j -> j.table(DSL.table("customers")).alias("b").on("customer_id", "id"))
.leftJoin(j -> j.table(DSL.table("pricing_rules"))
		.alias("c")
		.on("region_id", "region_id")          // c.region_id = f.region_id (LEFT side defaults to base)
		.on("b.segment", "segment"));          // c.segment   = b.segment   (LEFT side fully qualified)
```

The fully-qualified `b.segment` LEFT side declares an ON-clause dependency on `b`. The pruning
closure picks this up automatically: a query that asks only for `c.discount_pct` will keep both
`b` AND `c` in the materialised `FROM` (without that, `b` would be pruned and the SQL would
reference a dropped alias).

`parentAlias` is therefore a **convenience shortcut** for the common case where ON-clauses don't
need qualification, not a structural constraint of the join graph.

## Join pruning {#join-pruning}

> **The big win.** When most queries touch only a subset of the joins, eagerly composing the full
> `baseTable.leftJoin(...).leftJoin(...)` chain pays a per-join cost in the SQL engine even when
> the join contributes no columns to the `SELECT`. DuckDB, for example, pays a meaningful cost
> per `LEFT JOIN` even on rows that don't survive any filter. `PrunedJoinsJooqTableSupplier`
> prunes the `FROM` clause **per query** to the minimum set of joins reachable from the columns
> the query actually references.

How the closure works:

1. **Collect referenced columns** from the query — group-by columns, aggregator source columns,
   and every column appearing in a filter (shared `WHERE` or per-aggregator `FILTER (WHERE …)`).
2. **Look each column up** in the supplier's column→join index (built lazily from each
   `JoinNode`'s `columnsOverride` or — by default — a `SELECT * LIMIT 0` probe of the
   joined table).
3. **Mark the owning join needed**, then **walk the parent chain** transitively (snowflake
   ancestors come along) **plus every alias the ON-clause references** (for diamond joins).
4. **Force-include** any join declared with `.prunable(false)` — for joins whose presence
   changes row cardinality (an INNER-style join used as a filter, for instance) and must
   therefore not be elided.
5. **Materialise** the resulting subset back into a `Table<Record>`.

The decision is cached per referenced-column set (bounded LRU), so repeated queries with the
same column footprint don't re-walk the closure.

### Things that subvert pruning

- **`expressionColumnExtractor`-misses**: when a column reference is wrapped in a SQL expression
  the extractor doesn't recognise (custom dialect functions, deeply nested expressions), the
  supplier raises a strict-mode error rather than silently dropping the join. Configure a custom
  `IExpressionColumnExtractor` on the supplier when this bites.
- **`prunable(false)`** declared on a join opts it out of pruning entirely. Use this sparingly
  — only when the join's presence changes the result set semantically.
- **Calculated / non-pushdown filters** force the table layer to hoist the filter columns into
  every grouping set so the post-filter has per-row values to evaluate against — these columns
  show up as ON-clause leftovers and the closure correctly keeps the providing join alive.

## Column aliases

`JooqTableSupplierBuilder` records caller-facing column aliases via `withAlias(...)` — both as a
fluent setter on the join builder (per-join scope) and as a top-level setter on the schema
builder (post-join, scoped to the most recently declared join):

```java
.leftJoin(j -> j.table(DSL.table("customers"))
		.alias("c")
		.on("customer_id", "id")
		// Caller-facing alias `country` redirects to the actual table column `customer_country`.
		.withAlias("country", "customer_country"));
```

The alias is stored as `"c"."customer_country"` (jOOQ-escaped two-part `Name`) so the form is
unambiguous — it survives column names that contain dots or whitespace.

### Important: aliases must be combined with the cube's `ColumnsManager`

> **Read this twice.** `JooqTableSupplierBuilder`'s `aliasToOriginal` map is consumed by the
> table layer (pruning, `JooqTableWrapper.getColumns`) but is **NOT** automatically picked up by
> the cube's `ColumnsManager.aliaser` — the component that translates `queried → underlying` at
> SQL render time. If you don't wire the alias map into `ColumnsManager`, every alias-named
> column reaches the SQL renderer untranslated and the database rejects it with a
> "column not found" error.

The wiring you need at cube-build time:

```java
MapTableAliaser tableAliaser = MapTableAliaser.builder()
		.aliasToOriginals(schema.getAliasToOriginal())
		.build();

// If you have additional aliases beyond what the schema knows (e.g. cube-level renames,
// multi-table federation), compose them with the table-level aliaser. For a single source,
// pass `tableAliaser` directly; for multiple, use a `CompositeTableTranscoder`.
ITableAliaser combined = composeWithCustomAliasers(tableAliaser, /* user-provided aliasers */);

CubeWrapper cube = CubeWrapper.builder()
		.table(table)
		.forest(forest)
		.columnsManager(ColumnsManager.builder().aliaser(combined).build())
		.build();
```

The reverse direction — turning a `ColumnMetadata` back into the joined column it came from —
is handled automatically: `JooqTableWrapper.getColumns()` reads `getAliasToOriginal()` from the
supplier and attaches the caller-facing alias names to the corresponding `ColumnMetadata.aliases`
set, and renames any column whose bare name is shadowed by an alias to its qualified form
(e.g. `b.country` rather than `country` when an alias claims `country` for a different join).

### Bare-name collisions

When an alias claims a bare name already used by a real column on a **different** join, the
shadowed column is re-exposed in the column list under its qualified form `<owner>.<col>`. This
is intentional: the bare name is unambiguously the alias's target, and the shadowed column
stays reachable via its qualified form. The auto-registered ON-clause "aliases" (e.g. the bare
name `productId` registered as the canonical form on a natural-key join) are detected and do
NOT trigger renaming — they're identity mappings, not user-declared shadowings.

## Pitfalls and edge cases

- **Plain `DSL.table("name")` carries no declared fields.** The default `IJooqColumnsResolver`
  is a `SELECT * LIMIT 0` probe over the joined table — that works fine, but if you want the
  builder to not hit the DB for column resolution, either set `.providedColumns(Set.of(...))`
  on each join or use a derived table built via `DSL.values(...).asTable(alias, fieldNames...)`
  whose `fields()` carries the declared columns.
- **Identifiers needing quotes.** Column names with spaces, mixed case (when the dialect folds
  identifiers), or other special characters must be created with `DSL.quotedName(...)` so the
  underlying engine preserves them. The supplier's alias map then stores the jOOQ-escaped form
  (`"c"."country group"`) and queries through the alias work end-to-end. Direct column
  references can also use the bare-dotted form `c."country group"` if your dialect supports it,
  but the alias path is more portable.
- **`leftJoinConditions(...)`** registers a join with raw `Condition` objects — the supplier
  has no column-contract knowledge for that join, so it is forced to `prunable=false`. Prefer
  `.leftJoin(Consumer<JooqJoinBuilder>)` whenever possible.
- **Late `.leftJoin(...)`s** registered after queries have flowed need
  `PrunedJoinsJooqTableSupplier#invalidateAll()` to drop the stale column→alias index.

## Related reading

- [Tables](tables.md) — the simpler single-table path, where this article picks up.
- [CubeQueryEngine](cube-query-engine.md) — how the cube DAG and the table DAG interact, so you
  can understand where the `aliaser` translation step happens in the pipeline.
- [Optimisations](optimisations.md) — broader catalogue of query-time optimisations beyond join
  pruning.

