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
package eu.solven.adhoc.transformers.examples;

import java.util.Arrays;

import eu.solven.adhoc.aggregations.DivideCombination;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Filtrator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.IMeasureBagVisitor;
import eu.solven.adhoc.transformers.Unfiltrator;

/**
 * This defines a pattern, relying on multiple {@link IMeasure}, to produce one or multiple measures. The intermediate
 * measures are necessarily anonymous (really?).
 * 
 * It computes the ratio of current slice (e.g. color=blue) restricted to given additional filter (e.g. country=FRANCE),
 * over (e.g. for slice `color=blue`, it returns `color=blue&country=FRANCE / country=FRANCE`).
 * 
 * @author Benoit Lacelle
 *
 */
public class RatioOverSpecificColumnValueCompositor {
	public AdhocMeasureBag addTo(AdhocMeasureBag measureBag, String column, String value, String underlying) {
		String wholeMeasureName = "%s_%s=%s_whole".formatted(underlying, column, value);
		String sliceMeasureName = "%s_%s=%s_slice".formatted(underlying, column, value);
		String ratioMeasureName = "%s_%s=%s_ratio".formatted(underlying, column, value);

		return measureBag
				// Filter the specific country: if we were filtering color=red, this filters both color and country
				.addMeasure(Filtrator.builder()
						.name(sliceMeasureName)
						.underlying(underlying)
						.filter(ColumnFilter.isEqualTo(column, value))
						.build())

				// Filter the specific country: if we were filtering color=red, this filters only country
				.addMeasure(Unfiltrator.builder()
						.name(wholeMeasureName)
						.underlying(sliceMeasureName)
						.filterOnly(column)
						.build())

				// Filter the specific country: if we were filtering color=red, this returns (red&FR/FR)
				.addMeasure(Combinator.builder()
						.name(ratioMeasureName)
						.underlyings(Arrays.asList(sliceMeasureName, wholeMeasureName))
						.combinationKey(DivideCombination.KEY)
						.build());
	}

	/**
	 * 
	 * @param column
	 *            the filtered column
	 * @param value
	 *            the value filtered on given column
	 * @param underlying
	 *            an {@link IMeasureBagVisitor} adding an {@link IMeasure} computing the ratio for current slice of
	 *            `queryFilter&filter=column/filter=column`
	 * @return
	 */
	public IMeasureBagVisitor asCombinator(String column, String value, String underlying) {
		return new IMeasureBagVisitor() {

			@Override
			public AdhocMeasureBag addMeasures(AdhocMeasureBag adhocMeasureBag) {
				return RatioOverSpecificColumnValueCompositor.this.addTo(adhocMeasureBag, column, value, underlying);
			}
		};
	}
}
