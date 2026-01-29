package eu.solven.adhoc.fsst.v3;

import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCounters {
	@Test
	public void testCount() {
		Counters c = new Counters();

		c.incSingle(3);
		c.incPair(5, 7);

		{
			AtomicInteger codeHolder = new AtomicInteger(0);
			Assertions.assertThat(c.nextNotZero(codeHolder)).isEqualTo(1);
			Assertions.assertThat(codeHolder).hasValue(3);
		}

		{
			AtomicInteger codeHolder = new AtomicInteger(17);
			Assertions.assertThat(c.nextNotZero(codeHolder)).isEqualTo(0);
			Assertions.assertThat(codeHolder).hasValue(IFsstConstants.fsstCodeMax);
		}
	}

	@Test
	public void testReset() {
		Counters c = new Counters();

		c.incSingle(3);
		c.incPair(5, 7);

		c.reset();

		AtomicInteger codeHolder = new AtomicInteger(0);
		Assertions.assertThat(c.nextNotZero(codeHolder)).isEqualTo(0);
		Assertions.assertThat(codeHolder).hasValue(IFsstConstants.fsstCodeMax);

		Assertions.assertThat(c.getPairList()).isEmpty();

	}
}
