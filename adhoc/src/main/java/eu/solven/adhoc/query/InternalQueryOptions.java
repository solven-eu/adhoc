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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.TableQueryEngine;
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
@JsonSerialize(using = SimpleEnumSerializer.class)
public enum InternalQueryOptions implements IQueryOption {
	/**
	 * Given the {@link Set} of {@link TableQuery}, {@link TableQueryEngine} generally prefers querying the
	 * {@link ITableWrapper} a minimal set of {@link CubeQueryStep}, and computing within Adhoc induced results.
	 * 
	 * This option disables this behavior.
	 */
	DISABLE_AGGREGATOR_INDUCTION,;

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
