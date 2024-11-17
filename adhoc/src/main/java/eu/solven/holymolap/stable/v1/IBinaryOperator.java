package eu.solven.holymolap.stable.v1;

import java.util.function.BinaryOperator;

/**
 * Represents an operation upon two {@code Object}-valued operands, producing a {@code Object}-valued result.
 */
public interface IBinaryOperator extends BinaryOperator<Object> {
	/**
	 * 
	 * @return the value of an empty aggregate, or a row with no value for the aggregated column.
	 */
	Object neutral();

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
	Object apply(Object left, Object right);
}