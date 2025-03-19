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
package eu.solven.adhoc.measure.examples;

import java.util.Arrays;

import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.measure.IMeasureBagVisitor;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Technically-wise, this defines a pattern, relying on multiple {@link IMeasure}, to produce one or multiple measures.
 * The intermediate measures are necessarily anonymous (really?). In other words, it demonstrates how a pattern of
 * measures can be applied multiple times without duplication.
 * <p>
 * Functionally-wise, this evaluates the ratio
 *
 * @author Benoit Lacelle
 */
public class RatioOverCurrentColumnValueCompositor {
	public IAdhocMeasureBag addTo(IAdhocMeasureBag measureBag, String column, String underlying) {
		String wholeMeasureName = "%s_%s=%s_whole".formatted(underlying, column, "current");
		String sliceMeasureName = "%s_%s=%s_slice".formatted(underlying, column, "current");
		String ratioMeasureName = "%s_%s=%s_ratio_postcheck".formatted(underlying, column, "current");
		// We ensure the filter is compatible (e.g. a multi-selection with no groupBy would not be compatible)
		String validMeasureName = "%s_%s=%s_ratio".formatted(underlying, column, "current");

		return AdhocMeasureBag.edit(measureBag)

				// A Bucketor will make explicit what's the current value for requested column
				// Filter the specific country: if we were filtering color=red, this filters both color and country
				.measure(Bucketor.builder()
						.name(sliceMeasureName)
						.underlying(underlying)
						.groupBy(GroupByColumns.named(column))
						.build())

				// Filter the specific country: if we were filtering `country=FR&color=red`, this filters only
				// `country=FR`
				.measure(Unfiltrator.builder()
						.name(wholeMeasureName)
						.underlying(sliceMeasureName)
						.filterOnly(column)
						.build())

				// Computes the actual ratio from current slice (e.g. `country=FR&color=red`) over the parent slice
				// (e.g. `country=FR`)
				.measure(Combinator.builder()
						.name(ratioMeasureName)
						.underlyings(Arrays.asList(sliceMeasureName, wholeMeasureName))
						.combinationKey(DivideCombination.KEY)
						.build())

				// Ensure given column is expressed: if column is not expressed, we would not compute the ratio over the
				// missing column (we may also want to always return 1).
				.measure(Columnator.builder()
						.name(validMeasureName)
						.underlying(ratioMeasureName)
						.requiredColumn(column)
						.build())

				.build();
	}

	public IMeasureBagVisitor asCombinator(String column, String underlying) {
		return new IMeasureBagVisitor() {

			@Override
			public IAdhocMeasureBag addMeasures(IAdhocMeasureBag adhocMeasureBag) {
				return RatioOverCurrentColumnValueCompositor.this.addTo(adhocMeasureBag, column, underlying);
			}
		};
	}
}
