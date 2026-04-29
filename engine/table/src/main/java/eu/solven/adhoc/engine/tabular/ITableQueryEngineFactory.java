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
package eu.solven.adhoc.engine.tabular;

import java.util.Map;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableQueryPod;

/**
 * Part if {@link ICubeQueryEngine} dedicated to {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ITableQueryEngineFactory {

	Map<TableQueryStep, ICuboid> executeTableQueries(ITableQueryPod queryPod, QueryStepsDag queryStepsDag);

	/**
	 * Execute the {@code queryStepsDag} as a {@link StandardQueryOptions#DRILLTHROUGH} query: every database row is
	 * returned as an independent {@link ITabularView} entry, without any per-slice aggregation or measure
	 * transformation.
	 *
	 * <p>
	 * Default implementation throws {@link UnsupportedOperationException}; concrete factories that support DRILLTHROUGH
	 * (e.g. {@link TableQueryEngineFactory}) must override.
	 *
	 * @return the raw rows assembled into a {@link ITabularView}.
	 */
	default ITabularView executeDrillthrough(ITableQueryPod queryPod, QueryStepsDag queryStepsDag) {
		throw new UnsupportedOperationException(
				"DRILLTHROUGH is not supported by %s".formatted(this.getClass().getName()));
	}

}
