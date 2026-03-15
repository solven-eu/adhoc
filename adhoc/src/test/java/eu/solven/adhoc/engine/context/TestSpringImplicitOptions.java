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
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestSpringImplicitOptions {
	MockEnvironment env = new MockEnvironment();
	SpringImplicitOptions implicit = SpringImplicitOptions.builder().env(env).build();

	@Test
	public void test_noEnvOption_noQueryOption() {
		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_STEPS), true);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).containsExactly(InternalQueryOptions.TABLEQUERY_PER_STEPS);
	}

	@Test
	public void testEnv_hasEnvOption_logDefault() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_STEPS), true);

		Assertions.assertThat(implicit.logDefaultOptions()).containsExactly(InternalQueryOptions.TABLEQUERY_PER_STEPS);
	}

	@Test
	public void testEnv_hasEnvOption_UPPERCASE() {
		env.setProperty("adhoc.query.table." + InternalQueryOptions.TABLEQUERY_PER_STEPS, true);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).containsExactly(InternalQueryOptions.TABLEQUERY_PER_STEPS);
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption_multipleFalse() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_STEPS), false);
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_AGGREGATOR), false);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption_multipleTrue() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_STEPS), true);
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_AGGREGATOR), true);

		Assertions.assertThatThrownBy(() -> implicit.getOptions(CubeQuery.builder().build()))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining(InternalQueryOptions.TABLEQUERY_PER_STEPS.name())
				.hasMessageContaining(InternalQueryOptions.TABLEQUERY_PER_AGGREGATOR.name());
	}

	@Test
	public void testEnv_hasEnvOption_noQueryOption_unknownOption() {
		env.setProperty("adhoc.query.table." + "unknown_options", true);

		Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_hasQueryOption() {
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_STEPS), true);

		Set<IQueryOption> implicitOptions =
				implicit.getOptions(CubeQuery.builder().option(InternalQueryOptions.TABLEQUERY_PER_AGGREGATOR).build());
		Assertions.assertThat(implicitOptions).isEmpty();
	}

	@Test
	public void testEnv_hasEnvOption_hasDefaultOption() {
		implicit = SpringImplicitOptions.builder()
				.env(env)
				.defaultOption(InternalQueryOptions.TABLEQUERY_PER_STEPS)
				.build();

		// No env
		{
			Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
			Assertions.assertThat(implicitOptions).containsExactly(InternalQueryOptions.TABLEQUERY_PER_STEPS);
		}

		// with env
		env.setProperty(implicit.optionKey(InternalQueryOptions.TABLEQUERY_PER_STEPS), true);
		{
			Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
			Assertions.assertThat(implicitOptions).containsExactly(InternalQueryOptions.TABLEQUERY_PER_STEPS);
		}
	}

	@Test
	public void testEnv_multipleStandardOptionFromEnv() {
		implicit = SpringImplicitOptions.builder().env(env).build();

		env.setProperty(implicit.optionKey(StandardQueryOptions.CONCURRENT), true);
		env.setProperty(implicit.optionKey(StandardQueryOptions.EXPLAIN), true);
		{
			Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
			Assertions.assertThat(implicitOptions)
					.containsExactly(StandardQueryOptions.CONCURRENT, StandardQueryOptions.EXPLAIN);
		}
	}

	@Test
	public void testEnv_eachInternalIsKnown() {
		Stream.of(InternalQueryOptions.values()).forEach(option -> {
			// BEWARE Make sure each test is independent
			MockEnvironment env = new MockEnvironment();
			implicit = SpringImplicitOptions.builder().env(env).build();
			env.setProperty(implicit.optionKey(option), true);

			Set<IQueryOption> implicitOptions = implicit.getOptions(CubeQuery.builder().build());
			Assertions.assertThat(implicitOptions).containsExactly(option);
		});

	}
}
