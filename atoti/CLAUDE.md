# atoti/ — AI Agent Instructions

This module integrates Adhoc with **Atoti/ActivePivot**, a commercial in-memory OLAP database by ActiveViam.

---

## ⚠ Intellectual Property — Read Before Writing Any Code

Atoti/ActivePivot is **commercial software** with many **closed-source** components. ActiveViam holds full
copyright over its libraries, documentation, and any non-open-sourced material.

**Strict rules for this agent:**

1. **Do not reproduce ActivePivot source code.** Do not copy, paraphrase, or reconstruct any class, method,
   or algorithm from the closed-source jars, even partially.

2. **Do not reference private resources.** A resource is considered private if it is not reachable via a
   normal Google search — for example, a URL containing a unique token or customer-specific path that is not
   publicly indexed. Such links may be confidential (customer portal pages, authenticated Artifactory paths,
   private documentation behind a login). **Do not add such URLs to this project.**

3. **You may freely use any publicly indexed resource.** Documentation pages, Javadoc, GitHub repositories,
   and blog posts that appear in Google search results are fair game. The test: if a stranger with no ActiveViam
   account could find and open the URL via Google, it is public.

4. **All code in this module must be derivable from a specific public resource, and must comply with that
   resource's own license.** Do not assume Apache 2.0 — different ActiveViam/Atoti public resources carry
   different licenses (Apache 2.0, Unlicensed, or other). Always identify the source and its exact license
   before contributing code derived from it. See `atoti/README.MD` for the known resources and their licenses.

ActivePivot source code is **not available** — rely on the public resources below when working with its APIs.

---

## Key external resources

|                   Resource                   |                                 URL                                  |
|----------------------------------------------|----------------------------------------------------------------------|
| Atoti Server Javadoc (6.1.7)                 | https://docs.activeviam.com/products/atoti/server/6.1/javadoc/6.1.7/ |
| Atoti Server Javadoc (latest)                | https://docs.activeviam.com/products/atoti/server/latest/javadoc/    |
| Atoti technical docs                         | https://docs.activeviam.com/products/atoti/server/latest/docs/       |
| Memory Analysis Cube (open-source reference) | https://github.com/activeviam/mac                                    |
| Pivot Spring-Boot (open-source reference)    | https://github.com/activeviam/pivot-spring-boot                      |
| AutoPivot (open-source reference)            | https://github.com/activeviam/autopivot                              |
| ActiveViam Tools                             | https://github.com/activeviam/activeviam.github.io                   |

When you need to understand an ActivePivot type, fetch the relevant Javadoc page or search the open-source
reference repositories above — they use the same APIs.

---

## Concept mapping: ActivePivot → Adhoc

|              ActivePivot concept              |                                    Adhoc equivalent                                    |
|-----------------------------------------------|----------------------------------------------------------------------------------------|
| `ILocation` (wildcard)                        | `groupBy`                                                                              |
| `ISubCubeProperties` / `ICubeFilter`          | `filter`                                                                               |
| `IContextValue`                               | `CubeQueryBuilder.customMarker()` / `CubeQueryStep.getCustomMarker()`                  |
| `IQueryCache`                                 | `CubeQueryStep.getTransverseCache()`                                                   |
| `IPrefetcher`                                 | `IMeasureQueryStep.getUnderlyingSteps`                                                 |
| `IAggregationFunction`                        | `IAggregation`                                                                         |
| `AggregatedMeasure`                           | `IAggregator`                                                                          |
| `ABasicPostProcessor`                         | `Combinator`                                                                           |
| `ADynamicAggregationPostProcessorV2`          | `Partitionor`                                                                          |
| `AFilteringPostProcessorV2`                   | `Filtrator`                                                                            |
| `AAdvancedPostProcessorV2`                    | `Dispatchor`                                                                           |
| `ILevelInfo`                                  | column as `String` (typically `LevelIdentifier.level`)                                 |
| `LocationUtil.getCoordinate(location, level)` | `ISliceWithStep.sliceReader()` → `ISliceReader.extractCoordinate(level, String.class)` |
| `IRecordReader`                               | `ISlicedRecord`                                                                        |
| `IWritableCell`                               | `IValueProvider`                                                                       |

---

## Key IP constraint

Do **not** reproduce or suggest ActivePivot source code. All code in this module is derived solely from
the open-source repositories listed above (Apache 2.0). See `atoti/README.MD` for the full licensing note.
