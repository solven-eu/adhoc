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
package eu.solven.adhoc.measure.sum;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.solven.adhoc.data.cell.MultitypeCell;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.RequiredArgsConstructor;

/**
 * An {@link ICombination} define as the aggregation of underlying value with given {@link IAggregation}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class AggregationCombination implements ICombination {
	public static final String K_AGGREGATION_KEY = "aggregationKey";
	public static final String K_AGGREGATION_OPTIONS = "aggregationOptions";

	static final boolean DEFAULT_CUSTOM_IF_ANY_NULL = false;

	@Deprecated(
			since = "Unclear API. Should we rather use an IAggregation with a special behavior on null? (Which looks very bad)")
	public static final String K_CUSTOM_IF_ANY_NULL_OPERAND = "customIfAnyNullOperand";

	final IAggregation agg;

	// If true, any null underlying leads to a null output
	// If false, null underlyings are ignored
	final boolean customIfAnyNullOperand;

	public AggregationCombination(Map<String, ?> options) {
		IOperatorFactory operatorFactory =
				MapPathGet.<IOperatorFactory>getOptionalAs(options, StandardOperatorFactory.K_OPERATOR_FACTORY)
						.orElseGet(() -> StandardOperatorFactory.builder().build());

		Optional<Map<String, ?>> optAggregationOptions = MapPathGet.getOptionalAs(options, K_AGGREGATION_OPTIONS);
		String aggregationKey = MapPathGet.getRequiredString(options, K_AGGREGATION_KEY);
		agg = operatorFactory.makeAggregation(aggregationKey, optAggregationOptions.orElse(Map.of()));

		customIfAnyNullOperand = MapPathGet.<Boolean>getOptionalAs(options, K_CUSTOM_IF_ANY_NULL_OPERAND)
				.orElse(DEFAULT_CUSTOM_IF_ANY_NULL);
	}

	protected MultitypeCell makeMultitypeCell() {
		return MultitypeCell.builder().aggregation(agg).build();
	}

	@Override
	@SuppressWarnings("PMD.NullAssignment")
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		MultitypeCell refMultitype = makeMultitypeCell();

		IValueReceiver cellValueConsumer = refMultitype.merge();

		IValueReceiver proxyValueReceiver;
		AtomicBoolean hasNull;
		if (customIfAnyNullOperand) {
			hasNull = new AtomicBoolean();

			proxyValueReceiver = new IValueReceiver() {

				@Override
				public void onLong(long v) {
					cellValueConsumer.onLong(v);
				}

				@Override
				public void onDouble(double v) {
					cellValueConsumer.onDouble(v);
				}

				@Override
				public void onObject(Object v) {
					if (v == null) {
						hasNull.set(true);
					} else {
						cellValueConsumer.onObject(v);
					}
				}
			};
		} else {
			hasNull = null;
			proxyValueReceiver = cellValueConsumer;
		}

		int size = slicedRecord.size();
		for (int i = 0; i < size; i++) {
			slicedRecord.read(i).acceptReceiver(proxyValueReceiver);
		}

		if (customIfAnyNullOperand && hasNull.get()) {
			return oneUnderlyingIsNull();
		}

		return refMultitype.reduce();
	}

	protected IValueProvider oneUnderlyingIsNull() {
		return IValueProvider.NULL;
	}

}
