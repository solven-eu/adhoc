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

import eu.solven.adhoc.options.IQueryOption;
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
	 * Rely on `GROUPING SET` so that {@link ITableWrapper} evaluate itself various `GROUP BY`.
	 * 
	 * It leads to more work in the database, and less work into Adhoc.
	 * 
	 * @see eu.solven.adhoc.engine.tabular.splitter.InduceByGroupingSets
	 */
	INDUCE_BY_TABLE,

	/**
	 * Do not rely on `GROUPING SET` and request the finest granularity to {@link ITableWrapper}. Adhoc will induce the
	 * less granular steps.
	 * 
	 * It leads to less work in the database, and more work into Adhoc.
	 * 
	 * @see eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc
	 */
	INDUCE_BY_ADHOC,

	/**
	 * 
	 * @see eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouper
	 */
	TABLEQUERY_PER_OPTIONS,

	/**
	 * 
	 * @see eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperByAggregator
	 */
	TABLEQUERY_PER_AGGREGATOR,

	/**
	 * 
	 * @see eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouperNoGroup
	 */
	TABLEQUERY_PER_STEPS,

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
