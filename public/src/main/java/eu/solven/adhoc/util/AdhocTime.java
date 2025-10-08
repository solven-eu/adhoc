package eu.solven.adhoc.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Helps working with time through Adhoc.
 */
public class AdhocTime {

	public static Clock clock = Clock.systemUTC();

	public static Instant now() {
		return Instant.now(clock);
	}

	public static Duration untilNow(Instant start) {
		return Duration.between(start, now());
	}
}
