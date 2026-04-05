# Immutability

Adhoc uses immutable data structures pervasively — not primarily for thread safety, but because
immutability makes programs easier to reason about, test, and extend correctly.

---

## The core guarantee

An immutable object's state is fixed at construction time. No method can change it afterwards.
In Adhoc this is enforced by the `IImmutable` marker interface and verified at runtime: `put`,
`remove`, and `clear` throw `UnsupportedAsImmutableException` on every immutable collection.

---

## Benefits beyond concurrency

### Stability — values do not change under your feet

With a mutable map, passing it to a helper method creates a risk: the helper might modify it, or
the caller might modify it while the helper is still holding a reference. Tracking down such bugs
requires understanding the full call graph.

With an immutable map, this class of bug cannot exist. Once a slice is constructed, every piece of
code that holds a reference to it is guaranteed to see the same content, forever. You can pass
`ISlice` or `IAdhocMap` anywhere without defensive copies.

### Easier to follow the data flow

In a mutable system, understanding "what value does this field hold right now?" requires knowing
every code path that could have written to it since construction. In an immutable system the answer
is always: "the value it was given at construction." Reading the constructor is sufficient.

This is especially valuable in the measure evaluation pipeline, where the same slice passes through
many layers (filters, combinators, partitionors, shifters) without ever being modified. Each step
receives its own view; none can corrupt another's.

### Safe use as Map keys and Set elements

`HashMap` and `HashSet` require that a key's `hashCode()` and `equals()` do not change while the
key is in the collection. Mutable objects can silently violate this, producing keys that are
unfindable after a mutation.

`IAdhocMap` caches its hash code at first call (like `String`) because immutability guarantees it
will never become stale. Slices are therefore safe as `HashMap` keys, which the engine exploits
extensively in its result caches and groupBy projections.

### Caching without invalidation

Cached values derived from an immutable input are valid forever. The engine caches:

- **`retainAll` projections** — the mapping from `(full keyset, retained columns)` to a
  `RetainedKeySet` is memoised statically. With mutable maps this would be impossible.
- **Hash codes** — computed once, stored in the object.
- **Keyset projections in `ISliceFactory`** — interned key sets avoid allocating a new keyset
  object for every row that shares the same columns.

None of these caches would be correct without the immutability guarantee.

### Thread safety as a free side-effect

Immutable objects can be shared across threads without synchronisation because there is no mutable
state to protect. In Adhoc's concurrent query execution model (independent `CubeQueryStep` tasks
running on virtual threads), slices and result maps cross thread boundaries constantly. No locks
are needed.

---

## How immutability is applied in Adhoc

| Type | Guarantee | Where used |
|------|-----------|-----------|
| `IAdhocMap` | Fully immutable `SequencedMap`; hash code cached | Slice backing map; result storage |
| `ISlice` | Immutable coordinate set backed by `IAdhocMap` | Passed through the entire evaluation pipeline |
| `ISliceFilter` | Immutable filter tree (`AndFilter`, `OrFilter`, …) | Built once per query; shared across steps |
| `IMeasure` (Lombok `@Value`) | All measure definitions are value objects | Forest construction; DAG edges |
| `IMeasureForest` | Frozen after construction | Shared across concurrent query executions |
| `ImmutableList` / `ImmutableMap` (Guava) | Guava immutable collections throughout | Options, tags, underlyings lists |

---

## Trade-offs and when to copy

Immutability is not free. Operations that would mutate a mutable object must instead produce a new
object. Adhoc manages this through:

- **`retainAll`** — returns a new `IAdhocMap` (with cached projection metadata to keep allocation cheap).
- **`MaskedAdhocMap`** — adds columns to an existing map without copying it; the mask is a thin
  overlay that keeps the original untouched.
- **`toBuilder`** (Lombok) — measure builders support `toBuilder()` for cheap "copy with one field
  changed" construction.

The goal is to keep the hot path (evaluating millions of slices) allocation-light while preserving
the immutability guarantees at the API boundary.

---

## Further reading

- [Slice and IAdhocMap](slice.md) — how `ISlice` and `IAdhocMap` apply these contracts in practice
- [Concurrency](concurrency.md) — how immutability enables safe concurrent query execution
- [Optimisations](optimisations.md) — how cached projections and perfect hashing build on immutability
