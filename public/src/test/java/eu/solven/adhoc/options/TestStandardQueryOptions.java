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
package eu.solven.adhoc.options;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.pepper.unittest.PepperJackson3TestHelper;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

public class TestStandardQueryOptions {
	@Test
	public void testJackson() {
		String option = PepperJackson3TestHelper.verifyJackson(IQueryOption.class, StandardQueryOptions.EXPLAIN);

		Assertions.assertThat(option).isEqualTo("""
				"EXPLAIN"
				""".trim());
	}

	@Test
	public void testJackson_readLowerCase() {
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		ObjectMapper objectMapper = JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();

		Assertions.assertThat(objectMapper.readValue("\"eXpLaIn\"", IQueryOption.class))
				.isEqualTo(StandardQueryOptions.EXPLAIN);
	}

	@Test
	public void testConcurrentAndSequential() {
		Assertions.assertThat(StandardQueryOptions.CONCURRENT.isActive(Set.of())).isFalse();
		Assertions.assertThat(StandardQueryOptions.CONCURRENT.isActive(Set.of(StandardQueryOptions.CONCURRENT)))
				.isTrue();
		Assertions.assertThat(StandardQueryOptions.CONCURRENT.isActive(Set.of(StandardQueryOptions.SEQUENTIAL)))
				.isFalse();

		// If both are active, CONCURRENT is not active
		Assertions.assertThat(StandardQueryOptions.CONCURRENT
				.isActive(Set.of(StandardQueryOptions.CONCURRENT, StandardQueryOptions.SEQUENTIAL))).isFalse();
	}

	@Test
	public void testIsActive() {
		Assertions.assertThat(StandardQueryOptions.NON_BLOCKING.isActive(Set.of(StandardQueryOptions.NON_BLOCKING)))
				.isTrue();
		Assertions.assertThat(StandardQueryOptions.NON_BLOCKING.isActive(Set.of(StandardQueryOptions.BLOCKING)))
				.isFalse();
	}

	@Test
	public void testIsExplain_noOption() {
		Assertions.assertThat(IHasQueryOptions.noOption().isExplain()).isFalse();
		Assertions.assertThat(IHasQueryOptions.noOption().isDebug()).isFalse();
		Assertions.assertThat(IHasQueryOptions.noOption().isDebugOrExplain()).isFalse();
	}

	@Test
	public void testIsExplain_EXPLAIN() {
		IHasQueryOptions options = () -> Set.of(StandardQueryOptions.EXPLAIN);
		Assertions.assertThat(options.isExplain()).isTrue();
		Assertions.assertThat(options.isDebug()).isFalse();
		Assertions.assertThat(options.isDebugOrExplain()).isTrue();
	}
}
