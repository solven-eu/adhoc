# Unfiltrator

An `Unfiltrator` is a measure that **widens** the filter before evaluating its underlying — the
opposite of `Filtrator`. Where `Filtrator` adds constraints, `Unfiltrator` removes them from
specified columns, replacing them with `matchAll`.

The result is written to the original (un-widened) slice coordinates, so the caller's groupBy is
preserved.

---

## Motivation: ratio at a coarser granularity

A common reporting pattern is "what fraction of the total does this sub-set represent?"

Example: a user queries `sales GROUP BY city` with a filter `country=FR`. They also want the
country-level total for the denominator of a share-of-total measure. But the query only asks for
city, not country — and the country filter must remain in the numerator but be *dropped* from the
denominator.

```
Numerator:   SUM(sales) WHERE city=Paris AND country=FR   = 123
Denominator: SUM(sales) WHERE country=FR                  = 357   ← city filter removed
Share:       123 / 357 = 34.5 %
```

`Unfiltrator` handles the denominator: it receives the combined filter `city=Paris AND country=FR`,
strips `city`, and queries the underlying measure with `country=FR` alone.

---

## Builder syntax

```java
Unfiltrator.builder()
        .name("sales.fr_total")
        .underlying("sales")
        .column("city")          // suppress the city filter
        .mode(Mode.Suppress)     // default; explicit here for clarity
        .build()
```

### Mode.Suppress vs Mode.Retain

| Mode | Semantics |
|------|-----------|
| `Suppress` | Listed columns have their filter replaced by `matchAll`; all others are kept |
| `Retain` | Listed columns keep their filter; **all others** are replaced by `matchAll` |

`Retain` is useful when you want to express "keep only the country-level filter, drop everything
else" without having to enumerate every other column:

```java
Unfiltrator.builder()
        .name("sales.country_total")
        .underlying("sales")
        .column("country")
        .mode(Mode.Retain)    // keep country, suppress city, color, desk, …
        .build()
```

---

## The Filtrator → Unfiltrator → Combinator pattern

A complete share-of-total for a specific reference value (e.g. France) is assembled from three
measures. `RatioOverSpecificColumnValueCompositor` generates exactly this triple:

```java
// 1. Numerator — force country=FR on top of the query filter
Filtrator salesFrSlice = Filtrator.builder()
        .name("sales.fr_slice")
        .underlying("sales")
        .filter(ColumnFilter.matchEq("country", "FR"))
        .build();

// 2. Denominator — evaluate sales.fr_slice but drop everything except country
Unfiltrator salesFrWhole = Unfiltrator.builder()
        .name("sales.fr_whole")
        .underlying("sales.fr_slice")
        .column("country")
        .mode(Mode.Retain)        // keep the country=FR constraint, drop city, color, …
        .build();

// 3. Ratio
Combinator salesFrRatio = Combinator.builder()
        .name("sales.fr_ratio")
        .underlyings(List.of("sales.fr_slice", "sales.fr_whole"))
        .combinationKey(DivideCombination.KEY)
        .build();
```

**Query:** `sales.fr_ratio GROUP BY city, color WHERE country=FR`

| city | color | fr_slice | fr_whole | fr_ratio |
|------|-------|----------|----------|----------|
| Paris | blue | 80 | 357 | 22.4 % |
| Lyon | blue | 43 | 357 | 12.0 % |

The denominator is always the France total regardless of the `city` and `color` coordinates in
scope.

---

## The minimal-granularity pattern

Unfiltrator also solves the case where the **query filter itself** may be at varying granularity:
sometimes the user filters by country, sometimes by city. The measure should respect the coarsest
applicable level.

| User filter | Desired denominator behaviour |
|-------------|------------------------------|
| `country=FR` | Denominator = France total ✓ (no change needed) |
| `country=FR AND city=Paris` | Denominator = France total (drop city) |
| `country=FR AND city=Paris AND color=blue` | Denominator = France total (drop city and color) |

A single `Unfiltrator` with `Mode.Retain` on `country` handles all three cases uniformly:

```java
Unfiltrator.builder()
        .name("sales.country_total")
        .underlying("sales")
        .column("country")
        .mode(Mode.Retain)  // regardless of how many extra filters arrive, keep only country
        .build()
```

This is the standard approach for building hierarchical share-of-total measures today. See
[Hierarchies](hierarchies.md) for the broader discussion and the roadmap for native hierarchy
support.

---

## Comparison with related measure types

| | Filtrator | Unfiltrator | Shiftor |
|---|---|---|---|
| **Effect on filter** | ANDs an extra constraint | Removes constraint(s) | Replaces a value |
| **Direction** | Narrows | Widens | Redirects |
| **Typical use** | "Always include country=FR" | "Drop city, keep country" | "Fetch value from yesterday" |
| **Output coordinates** | Same as input | Same as input | Same as input |

---

## Further reading

- `TestAggregations_RatioSpecificCountry` — end-to-end share-of-total with Filtrator + Unfiltrator + Combinator
- `RatioOverSpecificColumnValueCompositor` — reusable helper that generates the three-measure pattern
- `RatioOverCurrentColumnValueCompositor` — variant using Partitionor for dynamic (non-hardcoded) reference values
- `SimpleFilterEditor` — utility methods `suppressColumn()` and `retainsColumns()` used internally
- [Shiftor](shiftor.md) — the generalisation that supports arbitrary filter transformations via `IFilterEditor`
- [Filtrator](filtrator.md) — the counterpart that narrows (ANDs) the filter instead of widening it
- [Hierarchies](hierarchies.md) — how to model multi-level dimensions and where Unfiltrator fits
- [Concepts → Measure archetypes](concepts.md) — overview of all measure types
