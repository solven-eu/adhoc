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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.query.table.TableQueryV3;

/**
 * Defines the grouping {@link CubeQueryStep}, driving the granularity of {@link TableQueryV3}.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ITableStepsGrouper {

	/**
	 * Maps a single inducer to the key that identifies its {@link TableQueryV3} group. Inducers sharing the same key
	 * are combined into one {@link TableQueryV3}.
	 */
	TableQueryStep tableQueryGroupBy(TableQueryStep inducer);

	/**
	 * Partitions all inducers into groups that will each be compiled into a single {@link TableQueryV3}. The default
	 * implementation delegates to {@link #tableQueryGroupBy(CubeQueryStep)} for each inducer. Implementations may
	 * override this method to apply a global optimisation across the full set (e.g. biclique decomposition to avoid
	 * cartesian-product waste in GROUPING SETS queries).
	 *
	 * @param inducers
	 *            the full set of leaf {@link CubeQueryStep}s that must be evaluated by the table layer
	 * @return a partition of {@code inducers}; every inducer must appear in exactly one group
	 */
	default Collection<? extends Set<TableQueryStep>> groupInducers(Set<TableQueryStep> inducers) {
		// TODO Introduce some AdhocStreams.groupingBy, with nice defaults (LinkedHashMap, ImmutableSet)
		return inducers.stream()
				.collect(Collectors
						.groupingBy(this::tableQueryGroupBy, LinkedHashMap::new, ImmutableSet.toImmutableSet()))
				.values();
	}

}
