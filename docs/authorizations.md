# Authorizations - Rights Management

See also: [Pivotable Security Model](pivotable-security.md) — covers the authentication layer (who the user is, how the SPA gets an `access_token`). This page only covers rights management **after** the user is authenticated.

Right-management is typically implemented by an `AND` operation combining the user `filter` and a filter based on user-rights.

This can be achieved through `IImplicitFilter`. A Spring-Security example is demonstrated in `TestImplicitFilter_SpringSecurity`.

## Common Questions

- How given performance are achieved?

`Adhoc` design delegates most of slow-to-compute sections to the underlying table. And 2020 brought a bunch of very fast database (e.g. DuckDB, RedShift, BigQuery).

- Can `Adhoc` handles indicators based on complex structures like `array` or `struct`?

Most databases handles aggregations over primitive types (e.g. SUM over doubles). `Adhoc` can aggregate any type, given you can implement your own `IAggregation`.
