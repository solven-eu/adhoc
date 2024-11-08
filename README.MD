# AdHoc - InMemory Transformation Pipeline

## Motivation

Make it easy to define a pipeline of transformation rules.

- The rules define a DAG/Directed-Acyclic-Graph, going from raw-data to complex measures. Intermediate nodes could also be used as measures.
- The DAG should be easily readable and modifiable but a human, not necessarily a developer.
- The DAG can express simple operations like `SUM` or `PRODUCT`, and complex operations like `GROUPBY`. 

## Related Projects

- SQLServer. We rely on many concepts from SQLServer to define our own abstractions.
- Apache Beam. Though [Beam](https://beam.apache.org/) seems less flexible to access intermediate results as intermediate measures.

## Aggregation Functions

See https://support.microsoft.com/en-us/office/aggregate-function-43b9278e-6aa7-4f17-92b6-e19993fa26df