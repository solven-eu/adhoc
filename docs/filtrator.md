# Filtrator

A `Filtrator` is a measure that evaluates its underlying with an additional, **hardcoded** filter
ANDed onto the current query filter. The result is written to the original slice coordinates.

It is conceptually a special case of `Shiftor` where the `IFilterEditor` always ANDs a fixed
predicate — common enough to deserve its own, simpler builder.

---

## Motivation: enforcing a constant constraint

Suppose a measure should always restrict to `country=FR`, regardless of what the user queries.
Without `Filtrator`, every caller would have to remember to add that constraint manually. With it,
the constraint is baked into the measure definition:

```java
Filtrator salesFr = Filtrator.builder()
		.name("sales.fr")
		.underlying("sales")
		.filter(ColumnFilter.matchEq("country", "FR"))
		.build();
```

When a user queries `sales.fr GROUP BY city`, the engine evaluates `SUM(sales) WHERE country=FR`
for each city — even if the query itself carries no country filter.

When the user *does* filter by country (say `country=DE`), the two filters are ANDed:
`country=FR AND country=DE`, which matches nothing and returns an empty result. The Filtrator
constraint is absolute; it cannot be overridden by the caller.

---

## How the AND is applied

The filter passed to the underlying measure is:

```
effective filter = query.filter AND filtrator.filter
```

Both sides are arbitrary `ISliceFilter` trees, so the hardcoded filter can be as complex as needed:

```java
// Restrict to FR or DE
Filtrator.builder()
		.name("sales.frade")
		.underlying("sales")
		.filter(OrFilter.of(
				ColumnFilter.matchEq("country", "FR"),
				ColumnFilter.matchEq("country", "DE")))
		.build()

// Restrict to a date range
Filtrator.builder()
		.name("sales.2025")
		.underlying("sales")
		.filter(AndFilter.of(
				ColumnFilter.isGreaterOrEqualTo("date", LocalDate.of(2025, 1, 1)),
				ColumnFilter.isLessThan("date",        LocalDate.of(2026, 1, 1))))
		.build()
```

---

## Filtrator inside the Filtrator → Unfiltrator → Combinator pattern

`Filtrator` is the natural numerator half of a hierarchical share-of-total. The denominator
is an `Unfiltrator` that removes all constraints except the one the `Filtrator` added, giving
the reference total for that constraint:

```java
// Numerator: force country=FR on top of the query filter
Filtrator salesFrSlice = Filtrator.builder()
		.name("sales.fr_slice")
		.underlying("sales")
		.filter(ColumnFilter.matchEq("country", "FR"))
		.build();

// Denominator: evaluate sales.fr_slice but strip everything except country
Unfiltrator salesFrWhole = Unfiltrator.builder()
		.name("sales.fr_whole")
		.underlying("sales.fr_slice")
		.column("country")
		.mode(Mode.Retain)          // keep country=FR, drop city, color, desk, …
		.build();

// Ratio
Combinator salesFrRatio = Combinator.builder()
		.name("sales.fr_ratio")
		.underlyings(List.of("sales.fr_slice", "sales.fr_whole"))
		.combinationKey(DivideCombination.KEY)
		.build();
```

`RatioOverSpecificColumnValueCompositor` generates exactly this triple from a single call.

---

## Comparison with related measure types

|                        |           Filtrator           |          Unfiltrator          |               Shiftor                |
|------------------------|-------------------------------|-------------------------------|--------------------------------------|
| **Effect on filter**   | ANDs a hardcoded constraint   | Removes constraint(s)         | Replaces a value via `IFilterEditor` |
| **Direction**          | Narrows                       | Widens                        | Redirects                            |
| **Complexity**         | Simple — no pluggable logic   | Simple — column list + mode   | Flexible — full `IFilterEditor`      |
| **Typical use**        | "Always enforce `country=FR`" | "Drop `city`, keep `country`" | "Fetch value from yesterday"         |
| **Output coordinates** | Same as input                 | Same as input                 | Same as input                        |

Use `Filtrator` when the constraint is constant and known at forest-build time. Reach for `Shiftor`
when the target filter must be computed from the current slice at query time (e.g. the previous
business day, or the parent node in a hierarchy).

---

## Further reading

- `RatioOverSpecificColumnValueCompositor` — reusable helper generating the Filtrator + Unfiltrator + Combinator triple
- `TestAggregations_RatioSpecificCountry` — end-to-end test of the share-of-total pattern
- [Unfiltrator](unfiltrator.md) — the counterpart that widens filters
- [Shiftor](shiftor.md) — the generalisation that redirects filters via a pluggable `IFilterEditor`
- [Hierarchies](hierarchies.md) — how Filtrator + Unfiltrator compose for hierarchical totals

