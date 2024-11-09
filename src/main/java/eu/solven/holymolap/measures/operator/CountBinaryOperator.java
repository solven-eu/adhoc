package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

/**
 * Count over {@link Long}
 * 
 * @author Benoit Lacelle
 *
 */
public class CountBinaryOperator implements ILongBinaryOperator {

	@Override
	public long applyAsLong(long left, long right) {
		return left + right;
	}

	@Override
	public long neutralAsLong() {
		return 0L;
	}
}