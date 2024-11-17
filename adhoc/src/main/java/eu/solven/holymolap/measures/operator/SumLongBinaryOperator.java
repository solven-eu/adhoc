package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

/**
 * Sum over {@link Long}
 * 
 * @author Benoit Lacelle
 *
 */
public class SumLongBinaryOperator implements ILongBinaryOperator, IDoubleBinaryOperator {

	@Override
	public long applyAsLong(long left, long right) {
		return left + right;
	}

	@Override
	public long neutralAsLong() {
		return 0L;
	}

	@Override
	public double applyAsDouble(double left, double right) {
		return left + right;
	}

	@Override
	public double neutralAsDouble() {
		return 0D;
	}

	@Override
	public Object apply(Object left, Object right) {
		if (left instanceof Integer && right instanceof Integer) {
			return ILongBinaryOperator.super.apply(left, right);
		} else if (left instanceof Long && right instanceof Long) {
			return ILongBinaryOperator.super.apply(left, right);
		} else {
			// Default to double behavior
			return IDoubleBinaryOperator.super.apply(left, right);
		}
	}

	@Override
	public Object neutral() {
		return neutralAsLong();
	}

}