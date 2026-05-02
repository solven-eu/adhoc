# Data Transfer / Primitive Management

> **See also**: [Type Inference](type-inference.md) for how Adhoc promotes narrower types (int→long, float→double) at the query level.

This section does **not** refer to data storage in Adhoc (as, by principle, Adhoc does not store data). But it is about mechanisms used in the library to manage data, especially primitive types.

The general motivation is:

- Prevent boxing/unboxing as much as possible: one should be able to rely on primitive type, especially in critical section of the engine. This enable better performance, and lower GC pressure.
- Focus on `long`, `double` and `Object`. `int` is managed as `long`. `float` is managed as `double`.
- Easy way to rely on plain Objects, until later optimization phases enabling `long` and `double` specific management.

## IValueReceiver

An `IValueReceiver` is subject to receiving data. Incoming data may be a `long`, a `double` or an `Object` (possibly `null`). The `Object` is not guaranteed not to be a `long` or a `double`.

## IValueProvider

An `IValueProvider` is subject to provide data. Outgoing data may be a `long`, a `double` or an `Object` (possibly `null`). The `Object` is not required not to be a `long` or a `double`.
