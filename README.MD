# AdHoc - Formulas for OLAP

[![Documentation](https://img.shields.io/badge/docs-solven.eu%2Fadhoc-blue)](https://www.solven.eu/adhoc/)
[![Maven Central](https://img.shields.io/maven-central/v/eu.solven.adhoc/adhoc)](https://central.sonatype.com/artifact/eu.solven.adhoc/adhoc)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=adhoc&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=adhoc)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=adhoc&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=adhoc)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=adhoc&metric=bugs)](https://sonarcloud.io/summary/new_code?id=adhoc)
[![codecov](https://codecov.io/gh/solven-eu/adhoc/graph/badge.svg?token=GFYB51WRFC)](https://codecov.io/gh/solven-eu/adhoc)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/solven-eu/adhoc/badge)](https://securityscorecards.dev/viewer/?uri=github.com/solven-eu/adhoc)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12108/badge)](https://www.bestpractices.dev/projects/12108)

## Motivation

Adhoc makes it easy to define/code/review [a la Excel](https://support.microsoft.com/en-us/office/Formulas-and-functions-294d9486-b332-48ed-b489-abe7d0f9eda9) formulas in the context of OLAP queries. It is compatible with a wide range of databases and query engines.

- The formulas define a DAG/Directed-Acyclic-Graph, going from abstract measures to aggregated measures. Intermediate formulas could also be used as measures.
- The DAG should be easily readable and modifiable by a human, not necessarily a developer.
- The DAG can express simple aggregations like `SUM` or `PRODUCT`, transformations like `GROUP BY`, filters like `WHERE country = FRA`.
- The DAg may also apply complex transformations (e.g. Many-to-many).
- Aggregated measures are evaluation by external databases (e.g. `GROUP BY` queries in SQL databases).

Most measures, including intermediate measures, hold a functional meaning. Hence, Adhoc can be seen as a semantic layer, enabling complex formulas over a snowflake SQL/!SQL schema, or a composite of snowflake schemas.


## Products

This repository includes 2 products:

- Adhoc, which is the core project as it holds the query-engine. It is a plain Java library (e.g. no API, no web-server).
- Pivotable, which is a referential web-application around Adhoc. It enables APIs (over [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html)) and a Single-Page-Application (with [VueJS](https://vuejs.org/)).

## Alternative projects

- Excel [Formulas](https://support.microsoft.com/en-us/office/Formulas-and-functions-294d9486-b332-48ed-b489-abe7d0f9eda9). While data are externalized from Adhoc, the tree of measures can be seen as cells referencing themselves through formulas.
- SQLServer Analysis Services [Measures](https://learn.microsoft.com/en-us/analysis-services/tabular-models/measures-ssas-tabular?view=asallproducts-allversions). We rely on many concepts from SQLServer to define our own abstractions.
- Apache Beam. Though [Beam](https://beam.apache.org/) seems less flexible to access intermediate results as intermediate measures.
- MongoDB [Aggregation Pipeline](https://www.mongodb.com/resources/products/capabilities/aggregation-pipeline).
- [DAX](https://learn.microsoft.com/en-us/dax/dax-overview) enables complex queries in Microsoft eco-system.
- [SquashQL](https://www.squashql.io/) is an SQL query-engine for OLAP, with a strong emphasis on its typescript UI.
- [Atoti PostProcessors](https://docs.activeviam.com/products/atoti/server/6.1.1/docs/cube/postprocessors/) is the standard Atoti way of building complex tree of measures on top of Atoti cubes.
- [Atoti DirectQuery](https://docs.activeviam.com/products/atoti/server/6.1/docs/directquery/getting_started/) enables Atoti to query an external Database instead of querying in own InMemory cubes.

### Similar but not drop-in replacement projects

- [Drill](https://drill.apache.org/): much stronger to be queried in SQL by BI Tools. Unclear strategy to define a large tree of measures.
- [Cube.js](https://cube.dev/docs/guides/recipes/data-modeling/filtered-aggregates), but ability to define a large tree of complex measure may not be sustainable.
- [DBT Metrics](https://docs.getdbt.com/docs/build/metrics-overview), but ability to define a large tree of complex measure may not be sustainable.
- [Polars](https://pola.rs/) enables data-processing on a single machine.

# Quick-start

## Hardware requirements

RAM: any JVM can run Adhoc, as Adhoc does not store data: it queries on-the-fly the underlying/external tables.
CPU: any JVM can run Adhoc. If multiple cores are available, Adhoc will takes advantage of them. But even a single-core JVM can run Adhoc queries smoothly.

## Set-up

1. Ensure you have JDK 21 available
2. Add a (`maven`/`gradle`) dependency to `eu.solven.adhoc:adhoc:0.0.18` (they are deployed to m2central: https://central.sonatype.com/artifact/eu.solven.adhoc/adhoc)
3. Define an `ITableWrapper`: it defines how Adhoc can access your data

Assuming your data is queryable with JooQ:

```
myTableWrapper = new JooqTableWrapper(JooqTableWrapperParameters.builder()
        .dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
        .table(yourJooqTableLike)
        .build());
```

For local `.parquet` files, it can be done with:

```
myTableWrapper = new JooqTableWrapper(JooqTableWrapperParameters.builder()
        .dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
        .table(DSL.table(DSL.unquotedName("read_parquet('myRootFolder/2025-*-BaseFacts_*.parquet', union_by_name=True)")))
        .build());
```

4. Define a `MeasureForest`: it defines the measures and the links between them, through their underlying measures.

An early-stage forest could look like:

```
Aggregator k1Sum = Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build();
Aggregator k2Sum = Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build();

Combinator k1PlusK2AsExpr = Combinator.builder()
        .name("k1PlusK2AsExpr")
        .underlyings(Arrays.asList("k1", "k2"))
        .combinationKey(ExpressionCombination.KEY)
        .combinationOptions(ImmutableMap.<String, Object>builder().put("expression", "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)").build())
        .build();

MeasureForest.MeasureForestBuilder forestBuilder = MeasureForest.builder();
forestBuilder.addMeasure(k1Sum);
forestBuilder.addMeasure(k2Sum);
forestBuilder.addMeasure(k1PlusK2AsExpr);
```

5. Defines an Adhoc Engine: it know how to execute a query given the measure relationships

```
CubeQueryEngine engine = CubeQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()).forest(forestBuilder.build()).build();
```

6. Define your query

```
ITabularView view = engine.execute(CubeQuery.builder().measure(k1SumSquared.getName()).debug(true).build(), jooqDb);
MapBasedTabularView mapBased = MapBasedTabularView.load(view);

Assertions.assertThat(mapBased.keySet().map(SliceAsMap::getCoordinates).toList())
        .containsExactly(Map.of());
Assertions.assertThat(mapBased.getCoordinatesToValues())
        .containsEntry(Map.of(), Map.of(k1SumSquared.getName(), (long) Math.pow(123 + 234, 2)));
```

7. Execute your query

# Documentation

The full documentation is deployed at [https://www.solven.eu/adhoc/](https://www.solven.eu/adhoc/).

# Concepts

See [docs/concepts.md](docs/concepts.md) for the full concepts documentation.

# Many-to-many

See [docs/many-to-many.md](docs/many-to-many.md) for details.

# Complex Aggregations (e.g. arrays for Value-at-Risk)

For for needs, the aggregation applies not over doubles or longs, but complex objects. For `Value-at-Risk`, the aggregated object is a `double[]` of constant length.

An example for such a use-case is demonstrated in `TestTableQuery_DuckDb_VaR`.

The ability to decompose along the scenarioIndex, or a scenarioName column, which are not defined in the table (which should provide one `double[]` per row) is achieved with a `Dispatchor`. Indeed, it can be seen as a `1-row-to-many-scenarioIndexes`.

# Type Inference

See [docs/type-inference.md](docs/type-inference.md) for details.

# Limitations

## Visual filters

Sometimes, one wants to filter the visible members along some columns, without filtering the actual query. Typically, one may want
to query the ratio `France/Europe` by filtering the `France` country, without restricting `Europe` to `France`-only. For now, this can not be easily done.

The underlying issue is that one mah have a column filtering `Country-with_firstLetterIsForG`. Assuming we have a measure returning `currentCountry/Europe`
where `currentCountry` is the country on the `Country` column, if we filter `Country-with_firstLetterIsForG=true` in the query, should we show
`France/(France+Germany)` or `France/Europe`?

## Potential solutions/designs

1. We may introduce a special `groupBy`, where we would express we groupBy `country` but only showing `Country-with_firstLetterIsForG=true`
2. We may introduce a special `filter`, stating that `Country-with_firstLetterIsForG=true` is a `Visual` filter. It resonates with https://learn.microsoft.com/en-us/sql/mdx/visualtotals-mdx?view=sql-server-ver16

# Debug / Investigations

See [docs/debug.md](docs/debug.md) for details.

# Tables

See [docs/tables.md](docs/tables.md) for details.

# Authorizations - Rights Management

See [docs/authorizations.md](docs/authorizations.md) for details.

# Data Transfer / Primitive Management

See [docs/data-transfer.md](docs/data-transfer.md) for details.

# IOperatorsFactory

See [docs/operators-factory.md](docs/operators-factory.md) for details.

# Filtering

See [docs/filtering.md](docs/filtering.md) for details.

# ETL (Extract-Transform-Load) emulation

Adhoc does not load data as its result are always based on results from underlying databases/`ITableWrapper`. Still, it may
be necessary to enable data customization similarly to ETL operations.

There is a few options to achieve such behavior.

## Transient JOINs with `JooqTableWrapper`

If you use `JooqTableWrapper` and the underlying database enables transient storage (e.g. like DuckDB), you could add a table and enrich your
JooQ table.

```java
public void createCustomTable(DSLSupplier dslSupplier) {
    DSLContext dslContext = dslSupplier.getDSLContext();

    dslContext.createTable("customTable")
        .column("customKey", SQLDataType.VARCHAR)
        .column("customColumn", SQLDataType.VARCHAR)

        .execute();

    dslContext.connection(c -> {
        DuckDBConnection duckDbC = (DuckDBConnection) c;

        DuckDBAppender appender = duckDbC.createAppender("main", "customTable");
        
        ImmutableMap.builder()
                .put("keyA", "customA")
                .put("keyB", "customB")
                .put("keyC", "customC")
                .build().forEach((key,value) -> {

                  try {
                    appender.beginRow();

                    appender.append(key);
                    appender.append(value);
                    
                    appender.endRow();
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                });
    });
}
```

and then add a join to your JooQ table:
```java
public void joinTable(Table baseTable) {
  baseTable.leftJoin(DSL.table("customTable")).on(DSL.field("rawValue").eq(DSL.field("customKey")));
}
```

The column `customColumn` or `"customTable"."customColumn"` can now be referenced as any other column.

## Calculated Column

`ICalculatedColumn` enables

# Optimizations

See [optimisations.md](https://github.com/solven-eu/adhoc/blob/master/docs/optimisations.md) for details about optimizations involved in Adhoc.

# Adhoc Unsafe

See [unsafe.md](https://github.com/solven-eu/adhoc/blob/master/docs/unsafe.md) for details about how to perform advanced tweaks in Adhoc.

# Roadmap / Limitations to lift soon

## Recent changes

[m2-central](https://central.sonatype.com/artifact/eu.solven.adhoc/adhoc)
Changes: [CHANGES.MD](https://github.com/solven-eu/adhoc/blob/master/CHANGES.MD)
Roadmap: [ROADMAP.MD](https://github.com/solven-eu/adhoc/blob/master/ROADMAP.MD)

## Limitations

Known limitations would generally trigger a [`NotYetImplementedException`](https://github.com/search?q=repo%3Asolven-eu%2Fadhoc%20NotYetImplementedException&type=code): please open a ticket to report your actual use-case for given scenario.

# Sponsors

## Profilers

Thanks [EJ Technologies for kindly providing an OpenSource license for JProfiler (their Java Profiler)](https://www.ej-technologies.com/jprofiler).

# Research

See [docs/research.md](docs/research.md) for details.