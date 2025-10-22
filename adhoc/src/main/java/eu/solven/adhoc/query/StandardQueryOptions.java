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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.table.composite.CompositeCubesTableWrapper;

/**
 * Various standard/not-exotic options for querying.
 * 
 * @author Benoit Lacelle
 *
 */
@JsonSerialize(using = SimpleEnumSerializer.class)
public enum StandardQueryOptions implements IQueryOption {
	/**
	 * Request for an unknown measure will treat it as if it returned only empty values.
	 *
	 * It is useful when a {@link eu.solven.adhoc.measure.IAdhocMeasureBag} refers a
	 * {@link eu.solven.adhoc.measure.IMeasure} which is be missing for any reason.
	 */
	UNKNOWN_MEASURES_ARE_EMPTY,

	/**
	 * When used, an {@link Exception} in a {@link eu.solven.adhoc.measure.step.ITransformator} evaluation leads to this
	 * {@link Exception} to be returned as {@link eu.solven.adhoc.measure.step.ITransformator} output.
	 */
	EXCEPTIONS_AS_MEASURE_VALUE,

	/**
	 * Force the {@link eu.solven.adhoc.query.cube.IAdhocQuery} to be executed with `explain==true`.
	 */
	EXPLAIN,

	/**
	 * Force the {@link eu.solven.adhoc.query.cube.IAdhocQuery} to be executed with `debug==true`.
	 */
	DEBUG,

	/**
	 * Enable concurrent execution of inner parts of a query.
	 */
	// https://stackoverflow.com/questions/1050222/what-is-the-difference-between-concurrency-and-parallelism
	CONCURRENT,

	/**
	 * Used to skip any cache mechanisms
	 */
	NO_CACHE,

	/**
	 * Keep {@link IAggregationCarrier} wrapped. Especially useful for {@link CompositeCubesTableWrapper}.
	 */
	AGGREGATION_CARRIERS_STAY_WRAPPED,

	/**
	 * Returns the rows from the table and do not process any measures
	 */
	// Current DT design is broken
	// `TableQueryEngineBootstrapped.toSortedColumns` will split the multi-column table output into a Set of
	// mono-columns. However, these columns can not be joined again, as multiple rows may have the same coordinate, from
	// different table queries. In the meantime, we keep this design, leading to each row having a single aggregator.
	// We may also need 2 kinds of DT: one standard DT, returning the notGroupedBy rows;and another DT returning the
	// groupedBy rows as returned by the table, without any measure processing.
	@Deprecated(since = "Not Ready")
	DRILLTHROUGH,

	;

	@JsonCreator
	public static IQueryOption forValue(String value) {
		return StandardQueryOptions.valueOf(value.toUpperCase(Locale.US));
	}
}
