/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
