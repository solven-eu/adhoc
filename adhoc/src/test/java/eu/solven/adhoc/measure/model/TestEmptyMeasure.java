package eu.solven.adhoc.measure.model;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;

public class TestEmptyMeasure {
	@Test
	public void testTags() {
		EmptyMeasure base = EmptyMeasure.builder().build();
		Assertions.assertThat(base.getTags()).containsExactly("technical");

		Assertions.assertThat(base.withTags(ImmutableSet.of("someTag")).getTags()).containsExactly("someTag");
	}

	@Test
	public void testUnderlyings() {
		EmptyMeasure base = EmptyMeasure.builder().build();

		ITransformatorQueryStep transformerStep =
				base.wrapNode(AdhocFactories.builder().build(), CubeQueryStep.builder().measure("m").build());

		// Fine on empty List
		{
			ISliceToValue onEmpty = transformerStep.produceOutputColumn(List.of());
			Assertions.assertThat(onEmpty.isEmpty());
		}

		Assertions.assertThatThrownBy(() -> transformerStep.produceOutputColumn(List.of(SliceToValue.empty())))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Received 1");
	}
}
