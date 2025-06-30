package eu.solven.adhoc.engine.tabular;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTableQueryOptimizer {
	CubeQueryStep step = CubeQueryStep.builder()
			.measure("m1")
			.groupBy(GroupByColumns.named("g", "h"))
			.filter(ColumnFilter.isEqualTo("c", "c1"))
			.build();

	TableQueryOptimizer optimizer = new TableQueryOptimizer();

	@Test
	public void testCanInduce_Trivial() {
		// Different measure by reference
		Assertions.assertThat(optimizer.canInduce(step, CubeQueryStep.edit(step).measure("m2").build())).isFalse();
		// Differnt measure with same name
		Assertions.assertThat(optimizer.canInduce(step, CubeQueryStep.edit(step).measure(Aggregator.sum("m1")).build()))
				.isTrue();

		// Less columns
		Assertions
				.assertThat(
						optimizer.canInduce(step, CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g")).build()))
				.isTrue();
		// More columns
		Assertions.assertThat(optimizer.canInduce(step,
				CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g", "h", "i")).build())).isFalse();

		// Different column same coordinate
		Assertions
				.assertThat(optimizer.canInduce(step,
						CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("d", "c1")).build()))
				.isFalse();
	}

	@Test
	public void testCanInduce_OrDifferentColumns() {
		Assertions.assertThat(optimizer.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step)
						.filter(OrFilter.or(ColumnFilter.isEqualTo("c", "c1"), ColumnFilter.isEqualTo("d", "c1")))
						.build(),
				// induced has only one of filters
				CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("c", "c1")).build()))
				// false because filtered columns are not groupedBy
				.isFalse();

		Assertions.assertThat(optimizer.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(OrFilter.or(ColumnFilter.isEqualTo("g", "c1"), ColumnFilter.isEqualTo("h", "c1")))
						.build(),
				// induced has only one of filters
				CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("g", "c1")).build()))
				// true because filtered columns are groupedBy: irrelevant `g` can be filtered.
				.isTrue();

		Assertions.assertThat(optimizer.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(OrFilter.or(ColumnFilter.isEqualTo("g", "c1"), ColumnFilter.isEqualTo("c", "c1")))
						.build(),
				// induced has only one of filters
				CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("g", "c1")).build()))
				// true because inducer has more rows than induced, and these rows can be filtered out (based on `g`)
				.isTrue();
	}

	@Test
	public void testCanInduce_AndDifferentColumns() {
		Assertions.assertThat(optimizer.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("c", "c1")).build(),
				// induced has only one of filters
				CubeQueryStep.edit(step)
						.filter(AndFilter.and(ColumnFilter.isEqualTo("c", "c1"), ColumnFilter.isEqualTo("d", "c1")))
						.build()))
				// false because filtered columns are not groupedBy
				.isFalse();

		Assertions.assertThat(optimizer.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("g", "c1")).build(),
				// induced has only one of filters
				CubeQueryStep.edit(step)
						.filter(AndFilter.and(ColumnFilter.isEqualTo("g", "c1"), ColumnFilter.isEqualTo("h", "c1")))
						.build()))
				// true because filtered columns are groupedBy
				.isTrue();

		Assertions.assertThat(optimizer.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step).filter(ColumnFilter.isEqualTo("g", "c1")).build(),
				// induced has only one of filters
				CubeQueryStep.edit(step)
						.filter(AndFilter.and(ColumnFilter.isEqualTo("g", "c1"), ColumnFilter.isEqualTo("c", "c1")))
						.build()))
				// false because inducer lacks information to filter along c
				.isFalse();
	}

	@Test
	public void testCanInduce_DifferentTopology() {
		Assertions.assertThat(optimizer.canInduce(
				// groupBy (g,h) matchAll
				CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g", "h")).filter(IAdhocFilter.MATCH_ALL).build(),
				// groupBy (g) filter (g)
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g"))
						.filter(ColumnFilter.isEqualTo("g", "someG"))
						.build()))
				// true because if inducer has laxer filter, there is enough groupBy to infer it
				.isTrue();

		Assertions.assertThat(optimizer.canInduce(
				// groupBy (g,h) matchAll
				CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g", "h")).filter(IAdhocFilter.MATCH_ALL).build(),
				// groupBy (g) filter (g)
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("c"))
						.filter(ColumnFilter.isEqualTo("c", "someC"))
						.build()))
				// false because inducer lax information about c
				.isFalse();
	}

	@Test
	public void testCanInduce_sameFilterNotGroupedBy() {
		Assertions.assertThat(optimizer.canInduce(
				// groupBy (g,h) filterX
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(ColumnFilter.isEqualTo("c", "someC"))
						.build(),
				// groupBy (g) filterX
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g"))
						.filter(ColumnFilter.isEqualTo("c", "someC"))
						.build()))
				// true because filter is identical
				.isTrue();
	}
}
