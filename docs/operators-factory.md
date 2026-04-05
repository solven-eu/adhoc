# IOperatorsFactory

`IOperatorsFactory` is the single extension point for injecting custom operator logic into the
measure evaluation pipeline. It is consulted whenever the engine needs to instantiate an operator
that is not hard-coded into the core library.

It produces four kinds of operators:

| Operator | Consumed by | Purpose |
|----------|------------|---------|
| `IAggregation` | `Aggregator` | Merge two partial results into one (e.g. `SUM`) |
| `ICombination` | `Combinator` | Combine a fixed set of underlying values at one slice |
| `IDecomposition` | `Dispatchor` | Split one input slice into multiple output entries |
| `IFilterEditor` | `Shiftor`, `Unfiltrator` | Transform an `ISliceFilter` before evaluation |

---

## Default resolution: class name as key

`StandardOperatorFactory` handles all built-in keys (`"SUM"`, `"COUNT"`, `"max"`, …) via a
`switch` statement. For any key it does not recognise, it treats the string as a fully-qualified
class name and instantiates the class via reflection:

1. If the class has a `Map<String, ?>` constructor, it is called with the operator's options map.
2. Otherwise the no-arg constructor is used.

This means any `ICombination`, `IAggregation`, `IDecomposition`, or `IFilterEditor` on the
classpath can be referenced without registration — just pass its class name as the key:

```java
Combinator.builder()
        .name("pnl.converted")
        .underlyings(List.of("pnl"))
        .combinationKey(FxCombination.class.getName())
        .build()
```

The limitation of reflection-based instantiation is that it cannot inject dependencies. A class
that needs a collaborator (a service, a configuration object, a Spring bean) cannot receive it
through a no-arg constructor.

---

## Use case: injecting a Spring bean into ICombination

Suppose `FxCombination` needs live FX rates from a `FxRateService` Spring bean. The bean cannot
be provided by reflection. The solution is a custom `IOperatorsFactory` that is itself a Spring
component and holds a reference to the `ApplicationContext` (or to specific beans directly).

### Option A — hold specific beans

The simplest approach: declare the dependencies explicitly as constructor fields.

```java
@Component
public class SpringOperatorFactory extends StandardOperatorFactory {

    private final FxRateService fxRateService;
    private final BusinessCalendarService calendarService;

    public SpringOperatorFactory(FxRateService fxRateService,
                                 BusinessCalendarService calendarService) {
        this.fxRateService = fxRateService;
        this.calendarService = calendarService;
    }

    @Override
    public ICombination makeCombination(String key, Map<String, ?> options) {
        if (FxCombination.KEY.equals(key)) {
            return new FxCombination(fxRateService);
        }
        if (BusinessDayCombination.KEY.equals(key)) {
            return new BusinessDayCombination(calendarService, options);
        }
        return super.makeCombination(key, options);  // fall back to reflection
    }
}
```

The `FxCombination` receives a fully initialised, Spring-managed `FxRateService`:

```java
public class FxCombination implements ICombination {

    public static final String KEY = "FX_LIVE";

    private final FxRateService fxRateService;

    public FxCombination(FxRateService fxRateService) {
        this.fxRateService = fxRateService;
    }

    @Override
    public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
        String ccy = (String) slice.getRaw("ccy");
        double rate = fxRateService.getRate(ccy, "USD");
        return ((Number) underlyingValues.get(0)).doubleValue() * rate;
    }
}
```

### Option B — delegate to the ApplicationContext

When the number of bean-aware operators grows, looking up from the `ApplicationContext` avoids
having to enumerate every dependency in the factory constructor:

```java
@Component
public class SpringOperatorFactory extends StandardOperatorFactory {

    private final ApplicationContext ctx;

    public SpringOperatorFactory(ApplicationContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public ICombination makeCombination(String key, Map<String, ?> options) {
        // If a bean with that name exists and implements ICombination, use it
        if (ctx.containsBean(key) && ctx.isTypeMatch(key, ICombination.class)) {
            return ctx.getBean(key, ICombination.class);
        }
        return super.makeCombination(key, options);
    }
}
```

Each operator is then registered as a named Spring bean:

```java
@Bean("FX_LIVE")
public ICombination fxCombination(FxRateService fxRateService) {
    return new FxCombination(fxRateService);
}
```

The measure references the bean name as its `combinationKey`:

```java
Combinator.builder()
        .name("pnl.usd")
        .underlyings(List.of("pnl"))
        .combinationKey("FX_LIVE")
        .build()
```

### Wiring the factory into the cube

Register the `SpringOperatorFactory` where the `CubeWrapper` or `CubeQueryEngine` is built:

```java
@Bean
public CubeWrapper cube(ITableWrapper table,
                        IMeasureForest forest,
                        SpringOperatorFactory operatorFactory) {
    return CubeWrapper.builder()
            .table(table)
            .forest(forest)
            .engine(CubeQueryEngine.builder()
                    .forest(forest)
                    .operatorFactory(operatorFactory)
                    .build())
            .build();
}
```

---

## Composing factories

`CompositeOperatorFactory` chains multiple factories: the first one that recognises a key wins.
This is useful when Spring-aware operators and reflection-based operators must coexist without
one factory subclassing the other:

```java
@Bean
public IOperatorsFactory operatorFactory(SpringOperatorFactory springFactory) {
    return CompositeOperatorFactory.of(springFactory, new StandardOperatorFactory());
}
```

---

## Further reading

- [Custom Aggregations](aggregation.md) — implementing `IAggregation` for domain objects
- [Partitionor](partitionor.md) — `IAggregation` used as the re-aggregation step after per-partition combination
- [Filtrator](filtrator.md) — `IFilterEditor` used to AND a hardcoded filter
- [Shiftor](shiftor.md) — `IFilterEditor` used to redirect a filter to a different slice
