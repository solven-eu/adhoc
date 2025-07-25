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

import java.util.List;

import eu.solven.adhoc.data.cell.MultitypeCell;
import eu.solven.adhoc.data.column.IMultitypeConstants;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.IBindableCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.combination.IHasTwoOperands;
import eu.solven.adhoc.measure.transformator.ICombinationBinding;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocBlackHole;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ICombination} which substract one measure from another.
 * 
 * @author Benoit Lacelle
 */
// https://dax.guide/op/subtraction/
@Slf4j
public class SubstractionCombination implements ICombination, IHasTwoOperands, IBindableCombination {

	public static final String KEY = "SUBSTRACTION";

	/**
	 * A {@link ICombinationBinding} specialized for {@link SubstractionCombination}.
	 * 
	 * @author Benoit Lacelle
	 */
	protected final class SubstractionBinding implements ICombinationBinding {
		final MultitypeCell receiver0 = MultitypeCell.builder().aggregation(new SumAggregation()).build();
		final IValueReceiver receiver1 = new IValueReceiver() {
			@Override
			public void onLong(long v) {
				switch (receiver0.getType()) {
				case IMultitypeConstants.MASK_EMPTY:
				case IMultitypeConstants.MASK_LONG:
					receiver0.onLong(-v);
					break;
				case IMultitypeConstants.MASK_DOUBLE:
					receiver0.onDouble(-v);
					break;
				case IMultitypeConstants.MASK_OBJECT:
					receiver0.onObject(negate(v));
					break;
				default:
					throw new IllegalArgumentException("Unexpected value: " + receiver0.getType());
				}
			}

			@Override
			public void onDouble(double v) {
				switch (receiver0.getType()) {
				case IMultitypeConstants.MASK_EMPTY:
				case IMultitypeConstants.MASK_LONG:
				case IMultitypeConstants.MASK_DOUBLE:
					receiver0.onDouble(-v);
					break;
				case IMultitypeConstants.MASK_OBJECT:
					receiver0.onObject(negate(v));
					break;
				default:
					throw new IllegalArgumentException("Unexpected value: " + receiver0.getType());
				}
			}

			@Override
			public void onObject(Object v) {
				switch (receiver0.getType()) {
				case IMultitypeConstants.MASK_EMPTY:
				case IMultitypeConstants.MASK_LONG:
				case IMultitypeConstants.MASK_DOUBLE:
				case IMultitypeConstants.MASK_OBJECT:
					receiver0.onObject(negate(v));
					break;
				default:
					throw new IllegalArgumentException("Unexpected value: " + receiver0.getType());
				}
			}
		};
		final IValueProvider consumer = new IValueProvider() {

			@Override
			public void acceptReceiver(IValueReceiver valueReceiver) {
				receiver0.acceptReceiver(valueReceiver);
			}
		};

		@Override
		public void reset(ISliceWithStep slice) {
			receiver0.clear();
		}

		@Override
		public IValueReceiver readUnderlying(int i) {
			if (i == 0) {
				return receiver0;
			} else if (i == 1) {
				return receiver1;
			} else {
				return AdhocBlackHole.getInstance();
			}
		}

		@Override
		public IValueProvider reduce() {
			return consumer;
		}
	}

	public static boolean isSubstraction(String operator) {
		return "-".equals(operator) || KEY.equals(operator) || operator.equals(SubstractionCombination.class.getName());
	}

	@Override
	public ICombinationBinding bind(int nbUnderlyings) {
		if (nbUnderlyings == 0) {
			return ICombinationBinding.empty();
		} else if (nbUnderlyings == 1) {
			// If `b` is missing in `a-b` , they one can simply return `a`.
			return ICombinationBinding.atIndex(0);
		}

		return new SubstractionBinding();
	}

	@Override
	public IValueProvider combine(ICombinationBinding binding, ISliceWithStep slice, ISlicedRecord slicedRecord) {
		binding.reset(slice);

		for (int i = 0; i < slicedRecord.size(); i++) {
			slicedRecord.read(i).acceptReceiver(binding.readUnderlying(i));
		}

		return binding.reduce();
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		if (slicedRecord.isEmpty()) {
			return IValueProvider.NULL;
		} else if (slicedRecord.size() == 1) {
			return slicedRecord.read(0);
		}

		IValueProvider left = slicedRecord.read(0);
		IValueProvider right = slicedRecord.read(1);

		return valueReceiver -> left.acceptReceiver(new IValueReceiver() {

			@Override
			public void onLong(long leftValue) {
				right.acceptReceiver(new IValueReceiver() {

					@Override
					public void onLong(long rightValue) {
						valueReceiver.onLong(leftValue - rightValue);
					}

					@Override
					public void onDouble(double rightValue) {
						valueReceiver.onDouble(leftValue - rightValue);
					}

					@Override
					public void onObject(Object rightValue) {
						if (rightValue == null) {
							valueReceiver.onLong(leftValue);
						} else {
							valueReceiver.onObject(substract(leftValue, rightValue));
						}
					}
				});
			}

			@Override
			public void onDouble(double leftValue) {
				right.acceptReceiver(new IValueReceiver() {

					@Override
					public void onLong(long rightValue) {
						valueReceiver.onDouble(leftValue - rightValue);
					}

					@Override
					public void onDouble(double rightValue) {
						valueReceiver.onDouble(leftValue - rightValue);
					}

					@Override
					public void onObject(Object rightValue) {
						if (rightValue == null) {
							valueReceiver.onDouble(leftValue);
						} else {
							valueReceiver.onObject(substract(leftValue, rightValue));
						}
					}
				});
			}

			@Override
			public void onObject(Object leftValue) {
				right.acceptReceiver(rightValue -> {
					if (rightValue == null) {
						valueReceiver.onObject(leftValue);
					} else {
						valueReceiver.onObject(substract(leftValue, rightValue));
					}
				});
			}
		});
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		if (underlyingValues.isEmpty()) {
			return null;
		} else if (underlyingValues.size() == 1) {
			return underlyingValues.getFirst();
		}

		Object left = underlyingValues.get(0);
		Object right = underlyingValues.get(1);

		if (right == null) {
			return left;
		} else if (left == null) {
			return negate(right);
		} else {
			return substract(left, right);
		}
	}

	protected Object negate(Object o) {
		if (o == null) {
			return null;
		} else if (AdhocPrimitiveHelpers.isLongLike(o)) {
			return -AdhocPrimitiveHelpers.asLong(o);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(o)) {
			return -AdhocPrimitiveHelpers.asDouble(o);
		}

		if (AdhocUnsafe.isFailFast()) {
			throw new NotYetImplementedException("Unclear expected behavior when negating not numbers: %s"
					.formatted(PepperLogHelper.getObjectAndClass(o)));
		} else {
			// TODO Should we return NaN ? Should we rely on some operators ? Should we rely on some interface?
			log.warn("Unclear expected behavior when negating not numbers: {}", PepperLogHelper.getObjectAndClass(o));
			return o;
		}
	}

	protected Object substract(Object left, Object right) {
		if (AdhocPrimitiveHelpers.isLongLike(left) && AdhocPrimitiveHelpers.isLongLike(right)) {
			return AdhocPrimitiveHelpers.asLong(left) - AdhocPrimitiveHelpers.asLong(right);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(left) && AdhocPrimitiveHelpers.isDoubleLike(right)) {
			return AdhocPrimitiveHelpers.asDouble(left) - AdhocPrimitiveHelpers.asDouble(right);
		} else {
			if (AdhocUnsafe.isFailFast()) {
				throw new NotYetImplementedException(
						"Unclear expected behavior when substracting not numbers: %s and %s".formatted(
								PepperLogHelper.getObjectAndClass(left),
								PepperLogHelper.getObjectAndClass(right)));
			} else {
				log.warn("Unclear expected behavior when substracting not numbers: {} and {}",
						PepperLogHelper.getObjectAndClass(left),
						PepperLogHelper.getObjectAndClass(right));
				return left + " - " + right;
			}
		}
	}

}
