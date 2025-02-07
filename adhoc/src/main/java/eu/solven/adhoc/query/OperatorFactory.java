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
package eu.solven.adhoc.query;

import eu.solven.adhoc.measure.IStandardOperators;

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

	// public static IMeasuredAxis sum(String axis) {
	// // return MeasuredAxis.builder() new MeasuredAxis(axis, OperatorFactory.SUM);
	// return null;
	// }
	//
	// public static IMeasuredAxis safeSum(String axis) {
	// // return new MeasuredAxis(axis, OperatorFactory.SAFE_SUM);
	// return null;
	// }
	//
	// public static IMeasuredAxis count(String axis) {
	// // return new MeasuredAxis(axis, OperatorFactory.COUNT);
	// return null;
	// }
	//
	// public static IMeasuredAxis cellCount(String axis) {
	// // return new MeasuredAxis(axis, OperatorFactory.CELLCOUNT);
	// return null;
	// }

}