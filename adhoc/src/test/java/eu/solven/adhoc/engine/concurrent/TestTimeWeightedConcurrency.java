package eu.solven.adhoc.engine.concurrent;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.testing.FakeTicker;

public class TestTimeWeightedConcurrency {
	FakeTicker ticker = new FakeTicker();
	TimeWeightedConcurrency twc = TimeWeightedConcurrency.builder().ticker(ticker).build();

	@Test
	void empty() {
		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(0D);
	}

	@Test
	void empty_someTime() {
		ticker.advance(10, TimeUnit.SECONDS);

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(0D);
	}

	@Test
	void testSingleTask_onThenOff() {
		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(0D);

		twc.startConcurrentTask();

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(1L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(0D);

		ticker.advance(2, TimeUnit.SECONDS);

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(1L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(0D);

		twc.stopConcurrentTask();

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(1D);

		ticker.advance(3, TimeUnit.SECONDS);

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(1D);

		twc.startConcurrentTask();

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(1L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(1D);

		ticker.advance(7, TimeUnit.SECONDS);

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(1L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(1D);

		twc.stopConcurrentTask();

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(1D);

		ticker.advance(11, TimeUnit.SECONDS);

		Assertions.assertThat(twc.getCurrentParallelism()).isEqualTo(0L);
		Assertions.assertThat(twc.getTimeWeightedParallelism()).isEqualTo(1D);
	}
}
