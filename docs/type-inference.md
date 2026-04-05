# Type Inference

> **See also**: [Data Transfer / Primitive Management](data-transfer.md) for how Adhoc handles primitive types at runtime.

## Ints and Longs

`int`s are generally treated as `long`s.

- Aggregations (e.g. `SUM`) will automatically turns `int` into `long`
- `EqualsMatcher`, `InMatcher` and `ComparingMatcher` will automatically turns `int` into `long`

## Floats and Doubles

`float`s are generally treated as `double`s.

- `SUM` will automatically turns `float` into `double`
- `EqualsMatcher`, `InMatcher` and `ComparingMatcher` will automatically turns `float` into `double`

## Numbers

- Aggregations should generally aggregates as `long`, else `double`.
