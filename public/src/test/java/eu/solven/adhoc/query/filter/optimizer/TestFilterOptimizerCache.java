package eu.solven.adhoc.query.filter.optimizer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;

public class TestFilterOptimizerCache {
	FilterOptimizer optimizer = FilterOptimizer.builder().build();
	FilterOptimizerWithCache optimizerWithCache = FilterOptimizerWithCache.builder().build();

	@Test
	public void testAnd() {
		ISliceFilter combined =
				FilterBuilder
						.and(ColumnFilter.matchIn("a", "a1", "a2", "a3"),
								ColumnFilter.matchIn("b", "b1", "b2", "b3"),
								FilterBuilder
										.or(ColumnFilter.matchIn("a", "a1", "a2", "a4"),
												ColumnFilter.matchIn("b", "b1", "b2", "b4"))
										.combine())
						.optimize(optimizerWithCache);

		Assertions.assertThat(optimizerWithCache.optimizedAndNegated.asMap()).hasSize(4);
		Assertions.assertThat(optimizerWithCache.optimizedAndNotNegated.asMap()).hasSize(11);
		Assertions.assertThat(optimizerWithCache.optimizedOrs.asMap()).hasSize(4);
		Assertions.assertThat(optimizerWithCache.optimizedNot.asMap()).hasSize(18);

		Assertions.assertThat(combined).hasToString("a=in=(a1,a2,a3)&b=in=(b1,b2,b3)&(a=in=(a1,a2)|b=in=(b1,b2))");
	}

}
