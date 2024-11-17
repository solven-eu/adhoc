package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

/**
 * Sum over {@link Double}, excluding NaN
 * 
 * @author Benoit Lacelle
 *
 */
public class SafeSumDoubleBinaryOperator implements IDoubleBinaryOperator {

	@Override
	public double applyAsDouble(double left, double right) {
		if (Double.isNaN(left)) {
			if (Double.isNaN(right)) {
				return neutralAsDouble();
			} else {
				return right;
			}
		} else if (Double.isNaN(right)) {
			return left;
		}
		return left + right;
	}

	@Override
	public double neutralAsDouble() {
		return 0D;
	}
}