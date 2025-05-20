package eu.solven.adhoc.measure.operator;

import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.measure.decomposition.LinearDecomposition;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumNotNaNAggregation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestCompositeOperatorsFactory {
    @Test
    public void testFallbackOnError() {
        IOperatorsFactory operatorsFactory = CompositeOperatorsFactory.builder()
                .operatorsFactory(new StandardOperatorsFactory())
                .operatorsFactory(DummyOperatorsFactory.builder().build())
                .build();

        Assertions.assertThat(operatorsFactory.makeAggregation(SumNotNaNAggregation.KEY)).isInstanceOf(SumNotNaNAggregation.class);
        Assertions.assertThat(operatorsFactory.makeAggregation("unknownKey")).isInstanceOf(SumAggregation.class);

        Assertions.assertThat(operatorsFactory.makeCombination(DivideCombination.KEY, Map.of())).isInstanceOf(DivideCombination.class);
        Assertions.assertThat(operatorsFactory.makeCombination("unknownKey", Map.of())).isInstanceOf(FindFirstCombination.class);

        Assertions.assertThat(operatorsFactory.makeDecomposition(LinearDecomposition.KEY, Map.of())).isInstanceOf(LinearDecomposition.class);
        Assertions.assertThat(operatorsFactory.makeDecomposition("unknownKey", Map.of())).isInstanceOf(SumAggregation.class);
    }
}
