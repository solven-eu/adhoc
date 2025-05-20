package eu.solven.adhoc.measure.transformator;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestCombinatorQueryStep {

    public static class CombinationWithUnderlyings implements ICombination, IHasUnderlyingNames {

        @Override
        public List<String> getUnderlyingNames() {
            return List.of("validUnderlying");
        }
    }

    @Test
    public void testCombinationHavingUnderlyings() {
        Combinator combinator = Combinator.builder().name("someName").combinationKey(CombinationWithUnderlyings.class.getName()).underlyings(Arrays.asList("invalidUnderlying")).build();

        Assertions.assertThat(combinator.getUnderlyingNames()).containsExactly("invalidUnderlying");

       ITransformator node =  combinator.wrapNode(new StandardOperatorsFactory(), CubeQueryStep.builder().measure("someName").build());

        Assertions.assertThat(node.getUnderlyingSteps()).singleElement().satisfies(step -> {
            Assertions.assertThat(step.getMeasure().getName()).isEqualTo("validUnderlying");
        });
    }
}
