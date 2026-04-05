# Adhoc Unsafe

Various inner parameters can be customized through static variables.

## AdhocUnsafe

Enables:
- Custom `adhocCommonPool` for CPU-bound work
- Custom `adhocDbPool` for external database queries (e.g. DuckDB) — bounded pool with backpressure to prevent overwhelming external databases
- Custom `maintenancePool` for background maintenance tasks

## AdhocUnsafeMap

Enables:
- Clearing cross-queries caches related to `.retainAll`

## AdhocFilterUnsafe

Enables:
- Custom default `IFilterOptimizer`
- Custom default `IFilterStripperFactory`

## AdhocFactoriesUnsafe

Enables:
- Custom factories as used by `AdhocFactories`

## AdhocFreezingUnsafe

Enables:
- Custom freezers. Normal customization goes with `IFreezingStrategy`.
- `AdhocFreezingUnsafe.setCheckPostCompression(true);` can be used to investigate data-corruption in compression algorithms.

## AdhocCaseInsensitivityUnsafe

**BEWARE** Case-Insensitivity is not fully supported yet. Only a very limited number of cases are functional.

Enable:
- switching to case-insensitivity