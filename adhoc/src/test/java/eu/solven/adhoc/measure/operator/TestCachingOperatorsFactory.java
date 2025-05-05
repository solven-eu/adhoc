package eu.solven.adhoc.measure.operator;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.AdhocIdentity;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;

public class TestCachingOperatorsFactory {
	IOperatorsFactory notCaching = Mockito.mock(IOperatorsFactory.class);
	CachingOperatorsFactory caching = CachingOperatorsFactory.builder().operatorsFactory(notCaching).build();

	@Test
	public void testMakeAggregation() {
		Mockito.when(notCaching.makeAggregation(SumAggregation.KEY, Map.of())).thenReturn(new SumAggregation());

		IAggregation aggregation1 = caching.makeAggregation(SumAggregation.KEY);
		IAggregation aggregation2 = caching.makeAggregation(SumAggregation.KEY);

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}

	@Test
	public void testMakeCombination() {
		Mockito.when(notCaching.makeCombination(SumCombination.KEY, Map.of())).thenReturn(new SumCombination());

		ICombination aggregation1 = caching.makeCombination(SumAggregation.KEY, Map.of());
		ICombination aggregation2 = caching.makeCombination(SumAggregation.KEY, Map.of());

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}

	@Test
	public void testMakeDecomposition() {
		Mockito.when(notCaching.makeDecomposition(AdhocIdentity.KEY, Map.of())).thenReturn(new AdhocIdentity());

		IDecomposition aggregation1 = caching.makeDecomposition(AdhocIdentity.KEY, Map.of());
		IDecomposition aggregation2 = caching.makeDecomposition(AdhocIdentity.KEY, Map.of());

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}

	@Test
	public void testMakeEditor() {
		Mockito.when(notCaching.makeEditor(AdhocIdentity.KEY, Map.of())).thenReturn(new AdhocIdentity());

		IFilterEditor aggregation1 = caching.makeEditor(AdhocIdentity.KEY, Map.of());
		IFilterEditor aggregation2 = caching.makeEditor(AdhocIdentity.KEY, Map.of());

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}
}
