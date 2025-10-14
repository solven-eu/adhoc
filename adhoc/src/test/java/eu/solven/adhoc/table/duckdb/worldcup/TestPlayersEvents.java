/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.duckdb.worldcup;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.example.worldcup.PlayersEvents;
import eu.solven.pepper.unittest.PepperJacksonTestHelper;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestPlayersEvents {
	@Test
	public void testHashcodeEauals() {
		EqualsVerifier.forClass(PlayersEvents.class).verify();
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		AtomicLongMap<Integer> goalEvents = AtomicLongMap.create();
		goalEvents.put(12, 23);
		String toString = PepperJacksonTestHelper.makeObjectMapper()
				.writeValueAsString(PlayersEvents.builder().typeToMinuteToCount("G", goalEvents).build());

		Assertions.assertThat(toString).isEqualTo("""
				{
				  "G" : {
				    "12" : 23
				  }
				}""");
	}

	@Test
	public void testMerge() throws JsonProcessingException {
		AtomicLongMap<Integer> goalEvents = AtomicLongMap.create();
		goalEvents.put(12, 23);

		AtomicLongMap<Integer> goalEvents2 = AtomicLongMap.create();
		goalEvents2.put(34, 45);

		AtomicLongMap<Integer> yellowEvents = AtomicLongMap.create();
		yellowEvents.put(56, 67);

		PlayersEvents merged = PlayersEvents.merge(PlayersEvents.builder().typeToMinuteToCount("G", goalEvents).build(),
				PlayersEvents.builder()
						.typeToMinuteToCount("G", goalEvents2)
						.typeToMinuteToCount("Y", yellowEvents)
						.build());

		String toString = PepperJacksonTestHelper.makeObjectMapper().writeValueAsString(merged);

		Assertions.assertThat(toString).isEqualTo("""
				{
				  "G" : {
				    "34" : 45,
				    "12" : 23
				  },
				  "Y" : {
				    "56" : 67
				  }
				}""");
	}

	@Test
	public void testMerge_sameMinute() throws JsonProcessingException {
		AtomicLongMap<Integer> goalEvents = AtomicLongMap.create();
		goalEvents.put(12, 23);

		AtomicLongMap<Integer> goalEvents2 = AtomicLongMap.create();
		goalEvents2.put(12, 45);

		AtomicLongMap<Integer> yellowEvents = AtomicLongMap.create();
		yellowEvents.put(12, 67);

		PlayersEvents merged = PlayersEvents.merge(PlayersEvents.builder().typeToMinuteToCount("G", goalEvents).build(),
				PlayersEvents.builder()
						.typeToMinuteToCount("G", goalEvents2)
						.typeToMinuteToCount("Y", yellowEvents)
						.build());

		String toString = PepperJacksonTestHelper.makeObjectMapper().writeValueAsString(merged);

		Assertions.assertThat(toString).isEqualTo("""
				{
				  "G" : {
				    "12" : 68
				  },
				  "Y" : {
				    "12" : 67
				  }
				}""");
	}
}
