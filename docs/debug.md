# Debug / Investigations

Typical errors with Adhoc are :
- Issue referencing underlying Table columns
- Unexpected data returned by underlying Table

Tools to investigate these issues are:
- Enable `debug` in your query: `CubeQuery.builder()[...].debug(true).build()`
- Enable `explain` in your query: `CubeQuery.builder()[...].explain(true).build()`

## Automated documentation

`ForestAsGraphvizDag` can be used to generate [GraphViz](https://graphviz.org/) `.dot` files given an `IMeasureForest`.

## Debug vs Explain

`StandardQueryOptions.DEBUG` will enable various additional `[DEBUG]` logs with `INFO` logLevel. It may also conduct additional operations (like executing some sanity checks), or enforcing some ordering to facilitate some investigations. It may lead to very poor performances.

`StandardQueryOptions.EXPLAIN` will provide additional information about the on-going query. It will typically log the query executed to the underlying table.

## Beyond log lines

Every `[DEBUG]` and `[EXPLAIN]` message is published as a structured `AdhocLogEvent` on
`IAdhocEventBus` before it reaches SLF4J. Subscribing to the bus instead of grepping log output
lets a host application capture debug traces, attach them to a query identifier, or display them
live in a UI — without changing the log layout. See the [EventBus](eventbus.md) page for the bus
contract, the built-in event types, and the recommended `AdhocEventBusHelpersUnsafe.safeWrapper`
injection pattern.
