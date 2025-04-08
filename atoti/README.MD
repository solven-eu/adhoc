# Adhoc for Atoti/ActivePivot

Atoti/ActivePivot is a very performant in-memory database.

https://www.atoti.io/

## Commercial Considerations

Atoti/ActivePivot is a commercial software, with strict intellectual property (IP) considerations. All the code here is
suggested from Open-Sourced repositories distributed by ActiveViam.

More specifically, this repository follows guidelines provided in:

- [Memory Analysis Cube](https://github.com/activeviam/mac) ([License: Apache 2.0](https://github.com/activeviam/mac/blob/main/LICENSE))
- [Atoti Spring-Boot Template](https://github.com/solven-eu/atoti-spring-boot-template) ([License: Apache 2.0](https://github.com/solven-eu/atoti-spring-boot-template/blob/6.1/LICENSE))
- [Pivot Spring-Boot](https://github.com/activeviam/pivot-spring-boot) ([License: Apache 2.0](https://github.com/activeviam/pivot-spring-boot/blob/6.1.0/LICENSE))
- [Atoti AutoPivot](https://github.com/activeviam/autopivot) ([License: Apache 2.0](https://github.com/activeviam/autopivot/blob/master/LICENSE))
- [Java API Migration Tool to migrate Atoti](https://github.com/activeviam/java-api-migration-tool) (License: Unlicensed)

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

They can generally be implemented through `Combinator`

## ADynamicAggregationPostProcessorV2

They can generally be implemented through `Bucketor`

## AFilteringPostProcessorV2

They can generally be implemented through `Filtrator`