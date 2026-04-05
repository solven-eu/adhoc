# Hierarchies

## What is a hierarchy?

In traditional OLAP, a **hierarchy** is an ordered set of levels within a dimension — for example
`Region → Country → City`. Aggregating at the `Region` level means summing all countries in that
region; drilling down to `Country` breaks that total apart by country; drilling further to `City`
splits by city.

Adhoc deliberately flattens this model: every column is a single level, and there are no built-in
multi-level hierarchies (see [Lexicon → Column](lexicon.md)). This keeps the query model simple —
a `GROUP BY` is just a set of columns, with no implicit roll-up rules.

Native multi-level hierarchy support is on the roadmap; see the section below.

---

## Modelling hierarchies today

Without native hierarchy support, multi-level relationships must be encoded in the **data** and
handled by **measure logic**.

### Flat representation in the table

The most common approach is to store each level as a separate column and populate every row with
values at all levels:

| city | country | region | sales |
|------|---------|--------|-------|
| Paris | FR | EMEA | 80 |
| Lyon | FR | EMEA | 43 |
| Berlin | DE | EMEA | 61 |
| New York | US | AMER | 120 |

Querying at any single level is a plain `GROUP BY`:

```java
CubeQuery.builder()
        .measure("sales")
        .groupBy(GroupByColumns.named("country"))   // or "region", or "city"
        .build()
```

This works because all three columns are present on every row. No hierarchy configuration is
needed.

### Hierarchical share-of-total with Unfiltrator

The flat representation also enables hierarchical ratios — *"city as a share of its country"* —
using the `Unfiltrator` pattern:

```java
// Denominator: drop city, keep country filter
Unfiltrator countryTotal = Unfiltrator.builder()
        .name("sales.country_total")
        .underlying("sales")
        .column("country")
        .mode(Mode.Retain)   // whatever the query filter contains, keep only country
        .build();

// Ratio
Combinator cityShare = Combinator.builder()
        .name("sales.city_share")
        .underlyings(List.of("sales", "sales.country_total"))
        .combinationKey(DivideCombination.KEY)
        .build();
```

When the user filters `country=FR AND city=Paris`, the `Retain` mode drops `city` and yields the
France total for the denominator.

### Hierarchical previous-value lookup with Shiftor

`Shiftor` can navigate a hierarchy if the shift logic understands the parent-child relationship.
Given a column `geo` that can hold either a city or a country value, a custom `IFilterEditor` can
walk up the hierarchy:

```java
public class GeoParentEditor implements IFilterEditor {

    private final GeoHierarchy hierarchy; // injected: knows FR is the parent of Paris

    @Override
    public ISliceFilter editFilter(ISliceFilter filter) {
        IValueMatcher matcher = FilterHelpers.getValueMatcher(filter, "geo");
        if (matcher instanceof EqualsMatcher eq) {
            String parent = hierarchy.parentOf((String) eq.getOperand()); // "Paris" → "FR"
            if (parent != null) {
                return SimpleFilterEditor.shift(filter, "geo", parent);
            }
        }
        return filter;
    }
}
```

The hierarchy data structure itself is a plain Java object — a `Map<String, String>` for simple
single-parent trees, or a more complex graph for DAG hierarchies (many-to-many roll-ups). Adhoc
imposes no specific structure; any object that your `IFilterEditor` can hold as a field works.

### Recommended structure for hierarchy metadata

For a simple strict hierarchy (each node has exactly one parent):

```java
// Map from child to parent at each level
Map<String, String> cityToCountry = Map.of("Paris", "FR", "Lyon", "FR", "Berlin", "DE");
Map<String, String> countryToRegion = Map.of("FR", "EMEA", "DE", "EMEA", "US", "AMER");
```

For a many-to-many hierarchy (e.g. a city belongs to multiple postal zones):

```java
// Map from child to set of parents
Map<String, Set<String>> cityToZones = Map.of("Paris", Set.of("Zone-A", "Zone-B"));
```

Many-to-many hierarchies naturally lead to the `Dispatchor` / [Many-to-many](many-to-many.md)
pattern: each input row is dispatched into multiple output coordinates.

---

## Roadmap: native multi-level hierarchy support

The ROADMAP tracks the following open item:

> **[Feature] Introduce the concept of multiLevel hierarchies**, hence implicitly the concept of
> slicing hierarchies. For now, each hierarchy is optional: no hierarchy is required in `groupBy`
> (or implicit on some default member).

Native hierarchy support would allow:

- Declaring `Region → Country → City` once in the forest/cube configuration, rather than encoding
  the roll-up logic in individual measures.
- Automatic `retainAll`-style widening when a query filter is at a finer level than the measure
  requires, replacing the manual `Unfiltrator` pattern.
- Query-level drill-down operations (navigate from `Region` to `Country` without rewriting the
  query).
- Implicit default member per level (e.g. `ALL` at the `Region` level when no filter is applied).

Until this feature ships, the flat-column + Unfiltrator + Shiftor approach described above covers
the most common hierarchical use cases. See [Unfiltrator](unfiltrator.md) and
[Shiftor](shiftor.md) for the concrete patterns.

---

## Further reading

- [Unfiltrator](unfiltrator.md) — widening filters for hierarchical totals and share-of-total ratios
- [Shiftor](shiftor.md) — navigating hierarchy levels via `IFilterEditor`
- [Many-to-many](many-to-many.md) — DAG hierarchies with `Dispatchor`
- [Lexicon → Column](lexicon.md) — why Adhoc flattens the traditional dimension/hierarchy/level model
