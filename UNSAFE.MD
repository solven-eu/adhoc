# Adhoc Unsafe

Various inner parameters can be customized through static variables.

## AdhocUnsafe

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
