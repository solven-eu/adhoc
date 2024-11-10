package eu.solven.adhoc.query;

import java.util.Arrays;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.greenrobot.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.dag.DAG;
import eu.solven.adhoc.eventbus.AdhocEventsToSfl4j;
import eu.solven.adhoc.transformers.Combinator;

public class TestAdhocQueryToUnderlyingQuery implements IAdhocTestConstants {
	EventBus eventBus = new EventBus();
	AdhocEventsToSfl4j toSlf4j = new AdhocEventsToSfl4j();
	DAG dag = new DAG(eventBus);

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);
	}

	@Test
	public void testSum() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(k1Sum);
		dag.addMeasure(k2Sum);

		Set<DatabaseQuery> output = dag.prepare(AdhocQueryBuilder.measure(k1Sum.getName()).build());

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(1).contains(k1Sum);
		});
	}

	@Test
	public void testSumOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(k1Sum);
		dag.addMeasure(k2Sum);

		Set<DatabaseQuery> output = dag.prepare(AdhocQueryBuilder.measure("sumK1K2").build());

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(2).contains(k1Sum, k2Sum);
		});
	}

	@Test
	public void testSum_SumOfSum() {
		dag.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyingMeasures(Arrays.asList("k1", "k2"))
				.transformationKey(SumTransformation.KEY)
				.build());

		dag.addMeasure(k1Sum);
		dag.addMeasure(k2Sum);

		Set<DatabaseQuery> output = dag.prepare(AdhocQueryBuilder.measure(k1Sum.getName(), "sumK1K2").build());

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(2).contains(k1Sum, k2Sum);
		});
	}
}
