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
package eu.solven.adhoc.table.duckdb.quantile;

import java.util.Map;
import java.util.Optional;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import smile.math.MathEx;
import smile.sort.QuickSelect;

public class CustomVaRCombination implements ICombination {

	public static final String KEY = "QUANTILE";

	public static final String P_QUANTILE = "quantile";

	final double quantile;

	public CustomVaRCombination(Map<String, ?> options) {
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
			quantile = 0.95D;
		}

		if (quantile < 0D || quantile > 1D) {
			throw new IllegalArgumentException("Expecting a quantile between 0 and 1");
		}
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		Object rawArray = IValueProvider.getValue(vc -> slicedRecord.read(0, vc));
		if (rawArray instanceof int[] array) {
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
