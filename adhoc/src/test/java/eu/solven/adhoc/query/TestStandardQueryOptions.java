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
package eu.solven.adhoc.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.data.tabular.TestMapBasedTabularView;

public class TestStandardQueryOptions {
	@Test
	public void testJackson() throws JsonProcessingException {
		String option = TestMapBasedTabularView.verifyJackson(IQueryOption.class, StandardQueryOptions.EXPLAIN);

		Assertions.assertThat(option).isEqualTo("""
				"EXPLAIN"
				""".trim());
	}

	@Test
	public void testJackson_readLowerCase() throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();

		Assertions.assertThat(om.readValue("\"eXpLaIn\"", IQueryOption.class)).isEqualTo(StandardQueryOptions.EXPLAIN);
	}

	@Disabled("TODO Custom options can not be (de)serialized properly for now")
	@Test
	public void testJackson_internalQueryOption() throws JsonProcessingException {
		String option = TestMapBasedTabularView.verifyJackson(IQueryOption.class,
				InternalQueryOptions.DISABLE_AGGREGATOR_INDUCTION);

		Assertions.assertThat(option).isEqualTo("""
				"DISABLE_AGGREGATOR_INDUCTION"
				""".trim());
	}
}
