# Tables

At the bottom of the DAG of cubeQuerySteps, the measures are measures evaluated by an external table, applying aggregation functions for given `GROUP BY` and `WHERE` clauses.

Here is an example of such a DAG:

```mermaidjs
graph TB
	cubeQuery[kpiA, kpiB on X by L]
	subgraph user query
	cubeQuery
	end
	cubeQuery --> measureA_cubeContext
	cubeQuery --> measureE_cubeContext
	subgraph cube DAG
	measureA_cubeContext[kpiA on Xby L]
	measureB_cubeContext[kpiB on X by L]
	measureC_cubeContext_v2[kpiC on X by L&M]
	measureD_cubeContext_v3[kpiD on X by L]
	measureE_cubeContext[kpiD on Y by L]
	measureF_cubeContext_v3[kpiD on X&Y by L&M]
	end
	measureA_cubeContext --> measureB_cubeContext
	measureB_cubeContext --> measureC_cubeContext_v2
	measureB_cubeContext --> measureD_cubeContext_v3
	measureE_cubeContext --> measureC_cubeContext_v2
	measureE_cubeContext --> measureF_cubeContext_v3
	subgraph table queries
	tableQuery_tableContext_v2[kpiD on X by L]
	tableQuery_tableContext_v3["kpiC, kpiD(on Y) on X by L&M"]
	end
	measureC_cubeContext_v2 --> tableQuery_tableContext_v3
	measureD_cubeContext_v3 --> tableQuery_tableContext_v2
	measureF_cubeContext_v3 --> tableQuery_tableContext_v3
```

## SQL with JooQ

SQL integration is provided with the help of JooQ. To query a complex star/snowflake schema (i.e. with many/deep joins), one should provide a `TableLike` expressing these `JOIN`s.

For instance:

```java
Table<Record> fromClause = DSL.table(DSL.name(factTable))
		.as("f")
		.join(DSL.table(DSL.name(productTable)).as("p"))
		.using(DSL.field("productId"))
		.join(DSL.table(DSL.name(countryTable)).as("c"))
		.using(DSL.field("countryId"));
```

Such snowflake schema can be build more easily with the help of `JooqSnowflakeSchemaBuilder`.

## Handling null (e.g. from failed JOINs)

See `eu.solven.adhoc.column.IMissingColumnManager.onMissingColumn(String)`
