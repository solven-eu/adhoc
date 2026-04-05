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
package eu.solven.adhoc.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAdhocTime {

	// Save and restore the global clock so tests don't interfere with each other
	Clock originalClock;
	ZoneOffset originalZone;

	@BeforeEach
	public void saveGlobals() {
		originalClock = AdhocTime.unsafeClock;
		originalZone = AdhocTime.unsafeZoneOffset;
	}

	@AfterEach
	public void restoreGlobals() {
		AdhocTime.unsafeClock = originalClock;
		AdhocTime.unsafeZoneOffset = originalZone;
	}

	@Test
	public void testNow_usesConfiguredClock() {
		Instant fixed = Instant.parse("2026-01-15T10:00:00Z");
		AdhocTime.unsafeClock = Clock.fixed(fixed, ZoneOffset.UTC);

		Assertions.assertThat(AdhocTime.now()).isEqualTo(fixed);
	}

	@Test
	public void testUntilNow_positiveDuration() {
		Instant t0 = Instant.parse("2026-01-15T10:00:00Z");
		Instant t1 = Instant.parse("2026-01-15T10:00:05Z");
		AdhocTime.unsafeClock = Clock.fixed(t1, ZoneOffset.UTC);

		Duration elapsed = AdhocTime.untilNow(t0);

		Assertions.assertThat(elapsed).isEqualTo(Duration.ofSeconds(5));
	}

	@Test
	public void testZoneOffset_default() {
		Assertions.assertThat(AdhocTime.zoneOffset()).isEqualTo(ZoneOffset.UTC);
	}

	@Test
	public void testZoneOffset_configurable() {
		ZoneOffset paris = ZoneOffset.ofHours(2);
		AdhocTime.unsafeZoneOffset = paris;

		Assertions.assertThat(AdhocTime.zoneOffset()).isEqualTo(paris);
	}
}
