package eu.solven.holymolap.stable.v1;

import java.util.function.DoubleBinaryOperator;

/**
 * Represents an operation upon two {@code double}-valued operands, producing a {@code double}-valued result.
 */
public interface IDoubleBinaryOperator extends DoubleBinaryOperator, IBinaryOperator {
	/**
	 * 
	 * @return the value of an empty aggregate, or a row with no value for the aggregated column.
	 */
	double neutralAsDouble();

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
	double applyAsDouble(double left, double right);

	@Override
	@Deprecated
	default public Object neutral() {
		return neutralAsDouble();
	}

	@Override
	@Deprecated
	default public Object apply(Object left, Object right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}
		return applyAsDouble((((Number) left).doubleValue()), (((Number) right).doubleValue()));
	}
}