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
package eu.solven.adhoc.table.duckdb.var;

import java.util.Map;
import java.util.Optional;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import smile.math.MathEx;
import smile.sort.QuickSelect;

/**
 * The VaR (e.g. quantile) computation, given an array.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "This is unsafe as it relies on `QuickSelect` which modify in-place")
public class ExampleVaRQuickSelectCombination implements ICombination {

	public static final String KEY = "EXAMPLEVAR_QUICKSELECT";

	public static final String P_QUANTILE = "quantile";

	final double quantile;

	public ExampleVaRQuickSelectCombination(Map<String, ?> options) {
		quantile = getQuantile(options);
	}

	public static double getQuantile(Map<String, ?> options) {
		double quantile;

		Optional<?> optRawQuantile = MapPathGet.getOptionalAs(options, P_QUANTILE);
		if (optRawQuantile.isPresent()) {
			Object rawQuantile = optRawQuantile.get();
			if (AdhocPrimitiveHelpers.isLongLike(rawQuantile)) {
				quantile = AdhocPrimitiveHelpers.asLong(rawQuantile) / 100D;
			} else if (AdhocPrimitiveHelpers.isDoubleLike(rawQuantile)) {
				quantile = AdhocPrimitiveHelpers.asDouble(rawQuantile);
			} else {
				quantile = Double.parseDouble(rawQuantile.toString());
			}
		} else {
			// Default quantile
			quantile = 0.95D;
		}

		if (quantile < 0D || quantile > 1D) {
			throw new IllegalArgumentException("Expecting a quantile between 0 and 1");
		}

		return quantile;
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		Object rawArray = IValueProvider.getValue(slicedRecord.read(0));
		if (rawArray instanceof int[] array) {
			if (array.length == 0) {
				return vc -> vc.onObject(null);
			}

			long output;
			if (quantile == 1D) {
				output = MathEx.max(array);
			} else {
				int index = (int) (array.length * quantile);

				// BEWARE This will edit array.
				output = QuickSelect.select(array, index);
			}

			return vc -> vc.onLong(output);
		} else if (rawArray instanceof double[] array) {
			if (array.length == 0) {
				return vc -> vc.onObject(null);
			}

			double output;
			if (quantile == 1D) {
				output = MathEx.max(array);
			} else {
				int index = (int) (array.length * quantile);

				// BEWARE This will edit array.
				output = QuickSelect.select(array, index);
			}

			return vc -> vc.onDouble(output);
		} else {
			throw new IllegalArgumentException(
					"Unexpected underlying: %s".formatted(PepperLogHelper.getObjectAndClass(rawArray)));
		}
	}

}
