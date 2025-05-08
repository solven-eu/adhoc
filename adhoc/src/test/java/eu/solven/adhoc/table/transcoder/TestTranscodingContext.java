package eu.solven.adhoc.table.transcoder;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestTranscodingContext {
	@Test
	public void testAddIdentity() {
		ITableTranscoder transcoder = Mockito.mock(ITableTranscoder.class, Mockito.CALLS_REAL_METHODS);

		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("c")).isEqualTo("c");
		Assertions.assertThat(context.queried("c")).containsExactly("c");

		Assertions.assertThat(context.underlyings()).containsExactlyInAnyOrder("c");
		Assertions.assertThat(context.estimateQueriedSize(Set.of("c"))).isEqualTo(1);

		// Implementation specific
		Assertions.assertThat(context.identity).contains("c");
		Assertions.assertThat(context.underlyingToQueried.asMap()).isEmpty();
	}

	@Test
	public void testAddMapped() {
		ITableTranscoder transcoder = Mockito.mock(ITableTranscoder.class, Mockito.CALLS_REAL_METHODS);

		Mockito.when(transcoder.underlying("cubeC")).thenReturn("tableC");

		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("cubeC")).isEqualTo("tableC");
		Assertions.assertThat(context.queried("tableC")).containsExactly("cubeC");
		Assertions.assertThat(context.estimateQueriedSize(Set.of("tableC"))).isEqualTo(1);

		Assertions.assertThat(context.underlyings()).containsExactlyInAnyOrder("tableC");

		// Implementation specific
		Assertions.assertThat(context.identity).isEmpty();
		Assertions.assertThat(context.underlyingToQueried.asMap()).containsEntry("tableC", Set.of("cubeC"));
	}

	@Test
	public void testAddIdentityAndMapped() {
		ITableTranscoder transcoder = Mockito.mock(ITableTranscoder.class, Mockito.CALLS_REAL_METHODS);

		Mockito.when(transcoder.underlying("cubeC")).thenReturn("tableC");
		Mockito.when(transcoder.underlying("tableC")).thenReturn("tableC");

		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("cubeC")).isEqualTo("tableC");
		Assertions.assertThat(context.underlying("tableC")).isEqualTo("tableC");
		Assertions.assertThat(context.queried("tableC")).contains("cubeC", "tableC").hasSize(2);
		Assertions.assertThat(context.estimateQueriedSize(Set.of("tableC"))).isEqualTo(2);

		Assertions.assertThat(context.underlyings()).containsExactlyInAnyOrder("tableC");

		// Implementation specific
		Assertions.assertThat(context.identity).contains("tableC");
		Assertions.assertThat(context.underlyingToQueried.asMap()).containsEntry("tableC", Set.of("cubeC"));
	}
}
