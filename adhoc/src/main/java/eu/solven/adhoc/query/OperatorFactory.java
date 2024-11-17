package eu.solven.adhoc.query;

import eu.solven.adhoc.aggregations.IStandardOperators;

public class OperatorFactory implements
		// IOperatorFactory,
		IStandardOperators {

	// @Override
	// public IBinaryOperator getBinaryOperator(String operator) {
	// if (SUM.equalsIgnoreCase(operator)) {
	// return IStandardDoubleOperators.SUM;
	// } else if (SAFE_SUM.equalsIgnoreCase(operator)) {
	// return IStandardDoubleOperators.SAFE_SUM;
	// } else if (COUNT.equalsIgnoreCase(operator)) {
	// return IStandardLongOperators.COUNT;
	// } else if (AVG.equalsIgnoreCase(operator)) {
	// return IStandardDoubleOperators.AVG;
	// } else {
	// throw new IllegalArgumentException("Unknown operator: " + operator);
	// }
	// }
	//
	// @Override
	// public IDoubleBinaryOperator getDoubleBinaryOperator(String operator) {
	// if (SUM.equalsIgnoreCase(operator)) {
	// return IStandardDoubleOperators.SUM;
	// // } else if (COUNT.equalsIgnoreCase(operator)) {
	// // return IStandardOperators.COUNT;
	// } else {
	// throw new IllegalArgumentException("Unknown operator: " + operator);
	// }
	// }
	//
	// @Override
	// public ILongBinaryOperator getLongBinaryOperator(String operator) {
	// if (SUM.equalsIgnoreCase(operator)) {
	// return IStandardLongOperators.SUM;
	// } else if (COUNT.equalsIgnoreCase(operator)) {
	// return IStandardLongOperators.COUNT;
	// } else {
	// throw new IllegalArgumentException("Unknown operator: " + operator);
	// }
	// }

//	public static IMeasuredAxis sum(String axis) {
//		// return MeasuredAxis.builder() new MeasuredAxis(axis, OperatorFactory.SUM);
//		return null;
//	}
//
//	public static IMeasuredAxis safeSum(String axis) {
//		// return new MeasuredAxis(axis, OperatorFactory.SAFE_SUM);
//		return null;
//	}
//
//	public static IMeasuredAxis count(String axis) {
//		// return new MeasuredAxis(axis, OperatorFactory.COUNT);
//		return null;
//	}
//
//	public static IMeasuredAxis cellCount(String axis) {
//		// return new MeasuredAxis(axis, OperatorFactory.CELLCOUNT);
//		return null;
//	}

}