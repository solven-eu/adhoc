/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Locale;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.TableQueryEngine;
import eu.solven.adhoc.engine.tabular.optimizer.TableQueryOptimizerSinglePerAggregator;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * Some {@link IQueryOption} which may enable/disable specific behavior, which should generally not be used by Users.
 * These may be optimizations to be disabled temporarily (e.g. due to a bug), and feature leading to bad performances
 * due to project specificities.
 * 
 * @author Benoit Lacelle
 *
 */
// @JsonSerialize(using = SimpleEnumSerializer.class)
public enum InternalQueryOptions implements IQueryOption {
	/**
	 * Given the {@link Set} of {@link TableQuery}, {@link TableQueryEngine} generally prefers querying the
	 * {@link ITableWrapper} a minimal set of {@link CubeQueryStep}, and computing within Adhoc induced results.
	 * 
	 * This option disables this behavior.
	 */
	ONE_TABLE_QUERY_PER_INDUCER,

	/**
	 * Enable the use of {@link TableQueryOptimizerSinglePerAggregator}, which will do one {@link TableQuery} per
	 * {@link Aggregator}, hence potentially reducing the number of queries to the database, but querying some
	 * information potentially useless (e.g. by querying some groupBy which is irrelevant for a part of the filter)
	 */
	@Deprecated(since = "Unclear if this should be a boolean, or if we should have some option exposed as an enum")
	ONE_TABLE_QUERY_PER_AGGREGATOR,

	/**
	 * This is the default behavior. Given a {@link Set} of {@link CubeQueryStep}, we find the sub-set which can induce
	 * all the others. This option is useful if the default behavior has been customized.
	 */
	@Deprecated(since = "Unclear if this should be a boolean, or if we should have some option exposed as an enum")
	ONE_TABLE_QUERY_PER_ROOT_INDUCER,

	;

	@JsonCreator
	public static InternalQueryOptions forValue(String value) {
		return InternalQueryOptions.valueOf(value.toUpperCase(Locale.US));
	}

	// https://github.com/FasterXML/jackson-databind/issues/5030
	// @JsonValue
	public String toValue() {
		return this.name();
	}
}
