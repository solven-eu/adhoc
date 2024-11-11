package eu.solven.adhoc.query;

import java.util.Arrays;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.transformers.Combinator;

public class TestAdhocQueryToUnderlyingQuery extends ADagTest implements IAdhocTestConstants {

	@Override
	public void feedDb() {
		// no need for data
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
