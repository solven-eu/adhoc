# Authorizations - Rights Management

See also: [Pivotable Security Model](pivotable-security.md) — covers the authentication layer (who the user is, how the SPA gets an `access_token`). This page only covers rights management **after** the user is authenticated.

Right-management is typically implemented by an `AND` operation combining the query's own filter and a filter derived from the user's rights. The user authored their query against the cube; the rights filter narrows the visible slice without the user knowing.

## `IImplicitFilter`

The contract:

```java
@FunctionalInterface
public interface IImplicitFilter {
	ISliceFilter getImplicitFilter(ICubeQuery query);
}
```

A single method, called once per query, returning the `ISliceFilter` the engine will combine with the query's own filter via AND. The returned filter does **not** need to include the query's own filter — the engine handles the combination. The query is passed in so an implementation can vary the filter by `customMarker` or by which columns/measures the query touches; most implementations ignore the argument and read from the ambient security context.

Wire it on the `IQueryPreparator` that the `CubeWrapper` uses:

```java
IQueryPreparator queryPreparator = StandardQueryPreparator.builder()
		.implicitFilter(new MyImplicitFilter())
		.build();

CubeWrapper cube = CubeWrapper.builder()
		.table(table)
		.engine(engine)
		.forest(forest)
		.queryPreparator(queryPreparator)
		.build();
```

From this point on, every `cube.execute(query)` call narrows the result to the slice authorised by `MyImplicitFilter`.

## Transcoding security roles into `ISliceFilter`

The natural source for the implicit filter is the framework's existing security context. `IImplicitFilter` is the seam: read the authorities/roles/claims that already represent the user, translate them into an `ISliceFilter`, return that.

`spring/src/test/java/eu/solven/adhoc/security/SpringSecurityAdhocImplicitFilter.java` is the canonical worked example. Its `getImplicitFilter` reads `SecurityContextHolder.getContext().getAuthentication()` and translates each `GrantedAuthority` to a filter operand:

- `ROLE_ADMIN` → `ISliceFilter.MATCH_ALL` (the admin sees everything; the OR below short-circuits).
- `ROLE_EUR` → `ColumnFilter.matchEq("ccy", "EUR")` (a static, role-encoded scope).
- `ROLE_color=<value>` → `ColumnFilter.matchEq("color", value)` (a dynamic, value-bearing role name — useful when the rights model has hundreds of values you do not want to hard-code).

The operands are combined with `FilterBuilder.or(...).optimize()` because the user has access to the **union** of the slices each role grants. The engine then ANDs this implicit filter with the query's own filter, so the user only sees rows that are both in their authorised slice and inside the query they actually asked for.

Important defaults illustrated by the example:

- **Null `Authentication` ⇒ `MATCH_NONE`** — a missing security context means deny-everything, not allow-everything. Always pick the safe direction here; an empty filter would silently expose the whole cube.
- **OR within rights, AND with query** — multiple roles broaden the visible slice (a user with `ROLE_EUR` and `ROLE_color=red` sees the union); the user-issued query then narrows that union.
- **Optimisation matters** — call `.optimize()` on the final filter. The result is reused across the query's whole DAG, and roles often produce filters that simplify cleanly (`MATCH_ALL OR x` collapses to `MATCH_ALL`, repeated `ColumnFilter.matchEq` on the same column merges into `IN`).

The full integration test `TestImplicitFilter_SpringSecurity` in the same package exercises every shape: admin sees the full grand-total, `ROLE_EUR` sees only EUR rows, multiple `ROLE_color=...` authorities expand the visible colour set, and the two dimensions combine.

The same pattern works for any rights provider — OAuth2 scopes, custom claims on a JWT, an external entitlements service — as long as you can render the user's permissions as an `ISliceFilter`.

## Common Questions

- How given performance are achieved?

`Adhoc` design delegates most of slow-to-compute sections to the underlying table. And 2020 brought a bunch of very fast database (e.g. DuckDB, RedShift, BigQuery).

- Can `Adhoc` handles indicators based on complex structures like `array` or `struct`?

Most databases handles aggregations over primitive types (e.g. SUM over doubles). `Adhoc` can aggregate any type, given you can implement your own `IAggregation`.
