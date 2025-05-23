package eu.solven.adhoc.table.transcoder;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.transcoder.CompositeTableTranscoder.ChainMode;

public class TestCompositeTableTranscoder {
	@Test
	public void testFirstNotNull() {
		MapTableTranscoder transcoder1 =
				MapTableTranscoder.builder().queriedToUnderlying("a1", "b1").queriedToUnderlying("a2", "b2").build();
		MapTableTranscoder transcoder2 =
				MapTableTranscoder.builder().queriedToUnderlying("b1", "c1").queriedToUnderlying("b3", "c3").build();

		CompositeTableTranscoder transcoder = CompositeTableTranscoder.builder()
				.chainMode(ChainMode.FirstNotNull)
				.transcoder(transcoder1)
				.transcoder(transcoder2)
				.build();

		Assertions.assertThat(transcoder.underlying("a1")).isEqualTo("b1");
		Assertions.assertThat(transcoder.underlying("b1")).isEqualTo("c1");
		Assertions.assertThat(transcoder.underlying("a2")).isEqualTo("b2");
		Assertions.assertThat(transcoder.underlying("b3")).isEqualTo("c3");
		Assertions.assertThat(transcoder.underlying("unknown")).isEqualTo(null);
	}

	@Test
	public void testApplyTry() {
		MapTableTranscoder transcoder1 =
				MapTableTranscoder.builder().queriedToUnderlying("a1", "b1").queriedToUnderlying("a2", "b2").build();
		MapTableTranscoder transcoder2 =
				MapTableTranscoder.builder().queriedToUnderlying("b1", "c1").queriedToUnderlying("b3", "c3").build();

		CompositeTableTranscoder transcoder = CompositeTableTranscoder.builder()
				.chainMode(ChainMode.ApplyAll)
				.transcoder(transcoder1)
				.transcoder(transcoder2)
				.build();

		Assertions.assertThat(transcoder.underlying("a1")).isEqualTo("c1");
		Assertions.assertThat(transcoder.underlying("b1")).isEqualTo("c1");
		Assertions.assertThat(transcoder.underlying("a2")).isEqualTo("b2");
		Assertions.assertThat(transcoder.underlying("b3")).isEqualTo("c3");
		Assertions.assertThat(transcoder.underlying("unknown")).isEqualTo(null);
	}
}
