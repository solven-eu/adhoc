# Adhoc for Atoti/ActivePivot

[Atoti/ActivePivot](https://www.atoti.io/) is a very performant in-memory database.

This module helps interacting between Adhoc and Atoti/ActivePivot. Main features are:

- Translation from ActivePivot measures descriptions to Adhoc forests. (It translated a tree of measures, but not the actual PostProcessor implementations).
- Query ActivePivot from Adhoc. (e.g. to backtest a translated PostProcessor over data stored in ActivePivot).

## Activeviam Intellectual Property Considerations

Atoti/ActivePivot is a commercial software, with strict intellectual property (IP) considerations. All the code here is
suggested from Open-Sourced repositories distributed by ActiveViam.

More specifically, this repository follows guidelines provided in:

- [Memory Analysis Cube](https://github.com/activeviam/mac) ([License: Apache 2.0](https://github.com/activeviam/mac/blob/main/LICENSE)) [fork](https://github.com/solven-eu/mac)
- [Atoti Spring-Boot Template](https://github.com/solven-eu/atoti-spring-boot-template) ([License: Apache 2.0](https://github.com/solven-eu/atoti-spring-boot-template/blob/6.1/LICENSE)) [fork](https://github.com/solven-eu/atoti-spring-boot-template)
- [Pivot Spring-Boot](https://github.com/activeviam/pivot-spring-boot) ([License: Apache 2.0](https://github.com/activeviam/pivot-spring-boot/blob/6.1.0/LICENSE)) [fork](https://github.com/solven-eu/atoti-spring-boot-template/tree/6.1)
- [Atoti AutoPivot](https://github.com/activeviam/autopivot) ([License: Apache 2.0](https://github.com/activeviam/autopivot/blob/master/LICENSE)) [fork](https://github.com/solven-eu/autopivot) [fork](https://github.com/solven-eu/autopivot)
- [Java API Migration Tool to migrate Atoti](https://github.com/activeviam/java-api-migration-tool) (License: Unlicensed) [fork](https://github.com/solven-eu/java-api-migration-tool)
- [ActiveViam Tools](https://github.com/activeviam/activeviam.github.io) ([License: Apache 2.0](https://github.com/activeviam/activeviam.github.io/blob/develop/LICENSE))  [fork](https://github.com/solven-eu/activeviam.github.io)
- [Atoti Notebooks](https://github.com/solven-eu/atoti-notebooks) ([License: Apache 2.0](https://github.com/activeviam/atoti-notebooks/blob/main/LICENSE)) [fork](https://github.com/solven-eu/atoti-notebooks)

[Licensing note](https://github.com/activeviam/autopivot?tab=readme-ov-file#licensing):

> The code of the AutoPivot application is open source, licensed under the Apache License 2.0. The AutoPivot application depends on the ActivePivot (commercial) software, the ActivePivot jar files distributed by ActiveViam must be available in the maven repository for the application to build. Running the AutoPivot application requires a license for the ActivePivot software. To use the ActiveUI frontend, the ActivePivot license must have the ActiveUI option enabled.

[CVaRC Accelerator](https://docs.activeviam.com/products/solutions/cvarc/4.0/online-help/dev/dev-getting-started/how-to-build-and-deploy-the-accelerator.html#set-up-artifactory-access):

```
<repository>
    <id>ActiveViamInternalRepository</id>
    <name>ActiveViam Internal Repository</name>
    <url>https://activeviam.jfrog.io/activeviam/activeviam-mvn-accelerators/</url>
</repository>
```

---

[Technical Documentation](https://docs.activeviam.com/products/atoti/server/latest/docs/start/license/)

> To use Atoti Server, you should have a valid license.

[Download](https://docs.activeviam.com/products/atoti/server/latest/docs/start/download/)

> A customer account is required to download the Atoti Server application.

> The released artifacts can be found on ActiveViam's JFrog Artifactory (https://activeviam.jfrog.io/artifactory/mvn/), with a different folder for each stable version.


## Major differences

1. Atoti maintains in-memory all or part of the considered data, while Adhoc does not keep any data In-Memory.
2. Atoti builds Hierarchies, as hierarchical organizations of levels, where each level maps to a column. Adhoc considers only 1-level hierarchies.
3. Atoti has a `hashCode-equals-toString-compareTo` constrain on levels. Adhoc has a `hashCode-equals` constrain on columns.
4. Atoti distributed cubes duplicates some data even in the single-jvm scenario. Adhoc composite cubes split queries and merge results on-the-fly.
5. Atoti requires to define a schema of the joined stores, and a mapping from these stores to the cube levels. Adhoc resolves columns on-the-fly.
6. Atoti defines concepts of `ILocation`, `ISubeCubeProperties` and `ICubeFilter`. Adhoc has `groupBy` (equivalent do `ILocation` wildcards) and filters.
7. Atoti can not easily do `OR` operations between levels during aggregation queries. Adhoc handles most filters as long as they are handled by the `IAdhocTableWrapper`.
8. Atoti offers querying through MDX. Adhoc does not speak MDX.

# Querying

A typical query in Atoti is based on:

- A `ILocation` expressing simpler filters (e.g. `level=value`, `level in (value1, value2, etc)` or `level is wildcard`)
- An `ISubCubeProperties` (which is mostly equivalent to a `ICubeFilter`) expressing filters on levels. These levels may or may not be expressed in the location. Filters on different levels are `AND`ed.

Let's consider as example:

- A `ILocation` like `country=FRANCE&city=WILDCARD`
- A `ISubCubeProperties` like `city in (Paris, Lyon) AND color=blue`

The equivalent Adhoc query looks like:

- GroupBy: `country&city`
- Filter: `country=FRANCE AND city in (Paris, Lyon) AND color=blue`

Note: Adhoc does not enable multi-level hierarchies nor slicing-hierarchies. They may be introduced at some point, through an additional mechanism expressing these hierarchies (i.e. through a dedicated object similarly to `IMissingColumnManager`, which centralize missing column management, instead of having each column having to define its specificities).

# Translating PostProcessors

Adhoc `IMeasure` is equivalent to ActivePivot measures.

## Tree of measures

One can translate existing ActivePivot measure trees with `AtotiMeasureToAdhoc`.

It requires to be run by Adhoc, hence in a JDK21 JVM. Though, ActivePivot5.X requires a JDK11 for most of its actions. Luckily, loading an `IActivePivotDescription` does not required a JDK11.

The simpler approach is to have a unit-test in your ActivePivot project, which provides one (or multiple) `IActivePivotDescription`. It can be turned into an Adhoc forest through:

```
AtotiMeasureToAdhoc translator = AtotiMeasureToAdhoc.builder().build();
IMeasureForest forest = converter.translate("someCubeName", someActivePivotDescription);
```

## Native measures

They can be automatically translated with `AtotiMeasureToAdhoc`.

1. `contributors.COUNT` is typically translated into `COUNT(*)`
2. `update.TIMESTAMP` is translated into a `MAX` over an unknown column.

## Pre-aggregated measures

ActivePivot `IAggregationFunction` can be translated into Adhoc `IAggregation`.

ActivePivot `AggregatedMeasure` can be translated into Adhoc `IAggregator`.

## Post-processed measure

The migration strategy is:
- Turns `ILevelInfo` into a column as a `String`, typically given `LevelIdentifier.level`
- Turns `LocationUtil.getCoordinate(location, level)` into `ISliceWithStep.sliceReader()` and `ISliceReader.extractCoordinate(level, String.class)`

## ABasicPostProcessor

`ABasicPostProcessor` can generally be implemented through `Combinator`.

[Javadoc](https://docs.activeviam.com/products/atoti/server/6.1/javadoc/6.1.7/com/activeviam/activepivot/core/ext/api/postprocessing/impl/BasicPostProcessor.html)

The migration strategy is:
- Turns `ABasicPostProcessor.evaluate` into `ICombination.combine`
- Operate an `ILocation` as a `ISliceWithStep`
- Operate `IRecordReader` as a `ISlicedRecord`
- Operate `IWritableCell` as a `IValueProvider`

## ADynamicAggregationPostProcessorV2

`ADynamicAggregationPostProcessorV2` can generally be implemented through `Partitionor`

[Javadoc](https://docs.activeviam.com/products/atoti/server/6.1/javadoc/6.1.7/com/activeviam/activepivot/core/ext/api/postprocessing/fwk/ADynamicAggregationPostProcessor.html)

The migration strategy is:
- Turns `ADynamicAggregationPostProcessor.leafLevels` into `Partitionor.groupBy`
- Turns `ADynamicAggregationPostProcessor.evaluateLeaf` into `ICombination.combine` 
- Turns extract coordinate from `ILocation`

## AFilteringPostProcessorV2

`AFilteringPostProcessorV2` can generally be implemented through `Filtrator`

[Javadoc](https://docs.activeviam.com/products/atoti/server/6.1/javadoc/6.1.7/com/activeviam/activepivot/core/ext/api/postprocessing/fwk/AFilteringPostProcessor.html)

The migration strategy is:
- Turns `FilteringPostProcessor.FILTERING_MEMBERS` into `ISliceFilter`

## AAdvancedPostProcessorV2

`AAdvancedPostProcessorV2` can often be implemented through `Dispatchor`, if the main logic is to dispatch an underlying slice into multiple ones.

# Concepts

## IContextValue

- IContextValue is a value defined dynamically (e.g. it can be customized on a per-query basis by the User), used to customize the behavior of measures.
- In Adhoc:
  - equivalent behavior can be implemented with a `CubeQueryBuilder.customMarker()` and `CubeQueryStep.getCustomMarker()`
  - Any `Object` (but `Optional`) can be a customMarker.
  - A typical `customMarker` is a plain `Map` or any POJO.

## IQueryCache

[Atoti Javadoc](https://docs.activeviam.com/products/atoti/server/javadoc/6.1.4/com/activeviam/activepivot/core/intf/api/query/IQueryCache.html)
[Atoti Wiki](https://docs.activeviam.com/products/atoti/server/6.0.3/docs/cube/context_values/#iquerycache)

- IQueryCache is a cache with a lifespan associated to current query.
- Equivalent concept in Adhoc is `CubeQueryStep.getTransverseCache()`.

## SumAggregation

[Atoti Wiki](https://docs.activeviam.com/products/atoti/server/6.0.3/docs/cube/aggregation-functions/#sum)

- *Adhoc* Aggregations are defined by the interface `IAggregation`
- *Adhoc* They are instantiated through a `IOperatorFactory`

- *Atoti* They are instantiated through a [Registry](https://docs.activeviam.com/products/atoti/server/6.0.3/docs/cube/aggregation-functions/#modifying-native-aggregation-functions)

## IPrefetcher

[Atoti Javadoc](https://docs.activeviam.com/products/atoti/server/latest/javadoc/com/activeviam/activepivot/core/intf/api/postprocessing/IPrefetcher.html)
[Atoti Wiki - Filtering](https://docs.activeviam.com/products/atoti/server/latest/docs/cube/filtering/)

- *Adhoc* defines the underlying CubeQueryStep through `IMeasureQueryStep.getUnderlyingSteps`

- *Atoti* defines the underlying *measure x location* through `IPrefetcher`