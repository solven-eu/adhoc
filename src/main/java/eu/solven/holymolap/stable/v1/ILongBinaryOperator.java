package eu.solven.holymolap.stable.v1;

import java.util.function.LongBinaryOperator;

/**
 * Represents an operation upon two {@code long}-valued operands, producing a {@code long}-valued result.
 */
public interface ILongBinaryOperator extends LongBinaryOperator, IBinaryOperator {
	/**
	 * 
	 * @return the value of an empty aggregate, or a row with no value for the aggregated column.
	 */
	long neutralAsLong();

	/**
	 * Applies this operator to the given operands.
	 *
	 * @param left
	 *            the first operand
	 * @param right
	 *            the second operand
	 * @return the operator result
	 */
	@Override
	long applyAsLong(long left, long right);

	@Override
	@Deprecated
	default public Object neutral() {
		return neutralAsLong();
	}

	@Override
	@Deprecated
	default public Object apply(Object left, Object right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}
		return applyAsLong((((Number) left).longValue()), (((Number) right).longValue()));
	}
}