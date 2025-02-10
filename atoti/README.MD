# Adhoc for Atoti/ActivePivot

Atoti/ActivePivot is a very performant in-memory database.

https://www.atoti.io/

## Major differences

1. Atoti maintains in-memory all or part of the considered data, while Adhoc does not keep anything In-Memory
2. Atoti builds Hierarchies, as hierarchical organizations of levels, where each level maps to a column. Adhoc considers only 1-level hierarchies.
3. Atoti has a `hashCode-equals-toString-compareTo` constrain on levels. Adhoc has a `hashCode-equals` constrain on columns.
4. Atoti distributed cubes duplicates some data even in the single-jvm scenario. Adhoc composite cubes split queries and merge results on-the-fly.

# Migrating PostProcessors

Adhoc `IMeasure` is equivalent to ActivePivot measures. One can migrate existing ActivePivot measure trees with `ActivePivotMeasuresToAdhoc`.

## Native measures

They can be automatically migrated with `ActivePivotMeasuresToAdhoc.convertNativeMeasure`.

1. `contributors.COUNT` is typically migrated into `COUNT(*)`
2. `update.TIMESTAMP` is migrated into a `MAX` over an unknown column.

## Pre-aggregated measures

ActivePivot `IAggregationFunction` can be migrated into Adhoc `IAggregation`.

ActivePivot `AggregatedMeasure` can be migrated into Adhoc `IAggregator`.

## Post-processed measure

## ABasicPostProcessor

They can generally be implemented through `ICombinator`

## ADynamicAggregationPostProcessorV2

They can generally be implemented through `IBucketor`