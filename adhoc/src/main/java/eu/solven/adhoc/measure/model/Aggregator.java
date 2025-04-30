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
package eu.solven.adhoc.measure.model;

import java.util.Map;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Used to transform an input column into a measure.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder(toBuilder = true)
@Jacksonized
public class Aggregator implements ITableMeasure, IHasAggregationKey, IAliasedAggregator {
	// The name/identifier of the measure
	@NonNull
	String name;

	@Singular
	ImmutableSet<String> tags;

	// The name of the underlying aggregated column
	@NonNull
	String columnName;

	@NonNull
	@Default
	String aggregationKey = SumAggregation.KEY;

	@Singular
	Map<String, Object> aggregationOptions;

	public static class AggregatorBuilder {
		String name;

		public AggregatorBuilder name(String name) {
			this.name = name;

			// columnName is set by default to the name
			if (columnName == null) {
				return columnName(name);
			} else {
				return this;
			}
		}
	}

	public static Aggregator sum(String column) {
		return Aggregator.builder().aggregationKey(SumAggregation.KEY).name(column).build();
	}

	public static AggregatorBuilder edit(Aggregator aggregator) {
		return Aggregator.builder()
				.name(aggregator.getName())
				.tags(aggregator.getTags())
				.columnName(aggregator.getColumnName())
				.aggregationKey(aggregator.getAggregationKey())
				.aggregationOptions(aggregator.getAggregationOptions());
	}

	public static Aggregator countAsterisk() {
		return Aggregator.builder()
				.name("count(*)")
				.columnName(ICountMeasuresConstants.ASTERISK)
				.aggregationKey(CountAggregation.KEY)
				.build();
	}

	public static Aggregator empty() {
		return Aggregator.builder()
				.name("empty")
				// columnName is irrelevant
				.columnName("empty")
				.aggregationKey(EmptyAggregation.KEY)
				.build();
	}

	@Override
	public Aggregator toAggregator() {
		return this;
	}

	@Override
	public String getAlias() {
		return getName();
	}

	@Override
	public Aggregator getAggregator() {
		return this;
	}
}
