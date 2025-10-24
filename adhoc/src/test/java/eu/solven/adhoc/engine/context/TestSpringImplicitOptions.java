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
package eu.solven.adhoc.engine.context;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestSpringImplicitOptions {
	MockEnvironment env = new MockEnvironment();
	SpringImplicitOptions implicit = new SpringImplicitOptions(env);

	@Test
	public void test_noEnvOption_noQueryOption() {
		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER), true);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).containsExactly(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER);
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption_multipleFalse() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER), false);
		env.setProperty(implicit.optionKey(InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR), false);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption_multipleTrue() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER), true);
		env.setProperty(implicit.optionKey(InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR), true);

		Assertions.assertThatThrownBy(() -> implicit.getOptions(CubeQuery.builder().build()))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER.name())
				.hasMessageContaining(InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR.name());
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption_unknownOption() {
		env.setProperty("adhoc.query.table." + "unknown_options", true);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_hasQueryOption() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER), true);

		Set<IQueryOption> implicitOptions = implicit
				.getOptions(CubeQuery.builder().option(InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR).build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}
}
