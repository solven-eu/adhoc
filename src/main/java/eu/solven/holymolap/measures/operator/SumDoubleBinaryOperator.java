package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

/**
 * Sum over {@link Double}
 * 
 * @author Benoit Lacelle
 *
 */
public class SumDoubleBinaryOperator implements IDoubleBinaryOperator {

	@Override
	public double applyAsDouble(double left, double right) {
		return left + right;
	}

	@Override
	public double neutralAsDouble() {
		return 0D;
	}
}