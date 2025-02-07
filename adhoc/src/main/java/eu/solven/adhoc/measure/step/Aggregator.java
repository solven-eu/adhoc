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
package eu.solven.adhoc.measure.step;

import java.util.Set;

import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.sum.SumAggregator;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Used to transform an input column into a measure.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class Aggregator implements IMeasure {
	// The name/identifier of the measure
	@NonNull
	String name;

	@Singular
	Set<String> tags;

	// The name of the underlying aggregated column
	String columnName;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	public String getColumnName() {
		if (columnName != null) {
			return columnName;
		} else {
			// The default columnName is the aggregator name
			return name;
		}
	}

	public static Aggregator sum(String column) {
		return Aggregator.builder().aggregationKey(SumAggregator.KEY).name(column).build();
	}

	public static AggregatorBuilder edit(Aggregator aggregator) {
		return Aggregator.builder()
				.name(aggregator.getName())
				.tags(aggregator.getTags())
				.columnName(aggregator.getColumnName())
				.aggregationKey(aggregator.getAggregationKey());
	}
}
