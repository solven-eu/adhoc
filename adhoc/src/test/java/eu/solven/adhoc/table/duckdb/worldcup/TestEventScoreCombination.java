package eu.solven.adhoc.table.duckdb.worldcup;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.example.worldcup.EventsScoreCombination;

public class TestEventScoreCombination {
	EventsScoreCombination combination = new EventsScoreCombination();

	@Test
	public void testScore() {
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(5L, 1L)))
				.isEqualTo(4L);
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(5L, 2L)))
				.isEqualTo(1L);

	}

	@Test
	public void testScore_null() {
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(5L, null)))
				.isEqualTo(5L);
		Assertions.assertThat(combination.combine(Mockito.mock(ISliceWithStep.class), Arrays.asList(null, 2L)))
				.isEqualTo(-4L);

	}
}
