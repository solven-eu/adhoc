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
package eu.solven.adhoc.engine;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import lombok.extern.slf4j.Slf4j;

/**
 * This tests concurrency in a very wide graph. Due to its width, there is a large probability for querySteps to be
 * requested multiple times in parallel. For instance, given a measure M, requesting multiple Nk combinators, each
 * requesting all Pk combinators, each requesting all Qk aggregators; then Pk and Qk would be requested in parallel.
 * 
 * While we may want to ensure each of these deep+concurrent steps to be evaluated only once, we want at least to be
 * sure not to fail in such case (as some code may not expect a step to be computed multiple times).
 */
@Slf4j
public class TestCubeQueryEngine_Concurrent2 extends ADagTest implements IAdhocTestConstants {
	int width = 16;

	@Override
	@BeforeEach
	public void feedTable() throws Exception {
		IntStream.range(0, width).forEach(i -> {
			table().add(Map.of("c", "c" + i, "k", i, "k_" + i, i));
		});
	}

	@Test
	public void testConcurrentTableQueries() {
		IntStream.range(0, width).forEach(i -> {
			forest.addMeasure(Aggregator.builder()
					.name("A_" + i)
					.columnName("k_" + i)
					.aggregationKey(SumAggregation.KEY)
					.build());
		});

		// each C_ is the sum of all K
		List<String> aggregators = IntStream.range(0, width).mapToObj(i -> "A_" + i).toList();
		IntStream.range(0, width).forEach(i -> {
			forest.addMeasure(Combinator.builder()
					.name("C_0_" + i)
					.underlyings(aggregators)
					.combinationKey(SumAggregation.KEY)
					.build());
		});

		// each D_ is the sum of all K
		List<String> combinators0 = IntStream.range(0, width).mapToObj(i -> "C_0_" + i).toList();
		IntStream.range(0, width).forEach(i -> {
			forest.addMeasure(Combinator.builder()
					.name("C_1_" + i)
					.underlyings(combinators0)
					.combinationKey(SumAggregation.KEY)
					.build());
		});

		List<String> combinators1 = IntStream.range(0, width).mapToObj(i -> "C_1_" + i).toList();
		forest.addMeasure(
				Combinator.builder().name("D").underlyings(combinators1).combinationKey(SumAggregation.KEY).build());

		// We want to make this this does not fail, typically around `CubeQueryEngine.onQueryStep` as the same subStep
		// may be queried by multiple dependent steps concurrently
		cube().execute(CubeQuery.builder().measure("D").option(StandardQueryOptions.CONCURRENT).build());
	}

}
