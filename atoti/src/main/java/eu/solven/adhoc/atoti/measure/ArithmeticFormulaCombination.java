/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.atoti.measure;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.sum.ProductAggregation;
import eu.solven.adhoc.measure.sum.ProductCombination;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Equivalent to {@link com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor}
 */
@Slf4j
public class ArithmeticFormulaCombination implements ICombination {
	final String formula;
	final Map<String, Integer> underlyingMeasuresToIndex = new HashMap<>();

	public ArithmeticFormulaCombination(Map<String, ?> options) {
		// e.g. `aggregatedValue[CDS Composite Spread5Y],double[10000],*`
		formula = MapPathGet.getRequiredString(options, ArithmeticFormulaPostProcessor.FORMULA_PROPERTY);

		String[] elements = formula.split(",");

		Stream.of(elements)
				.filter(s -> s.startsWith("aggregatedValue[") && s.endsWith("]"))
				.map(s -> s.substring("aggregatedValue[".length(), s.length() - "]".length()))
				.forEach(underlyingMeasure -> underlyingMeasuresToIndex.put(underlyingMeasure,
						underlyingMeasuresToIndex.size()));
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		List<Object> pendingOperands = new LinkedList<>();

		String[] elements = formula.split(",");

		for (int i = 0; i < elements.length; i++) {
			String s = elements[i];

			Object operandToAppend;
			if (s.startsWith("aggregatedValue[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("aggregatedValue[".length(), s.length() - "]".length());
				int measureIndex = underlyingMeasuresToIndex.get(underlyingMeasure);
				operandToAppend = underlyingValues.get(measureIndex);
			} else if (s.startsWith("double[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("double[".length(), s.length() - "]".length());
				operandToAppend = Double.parseDouble(underlyingMeasure);
			} else if (s.startsWith("int[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("int[".length(), s.length() - "]".length());
				operandToAppend = Integer.parseInt(underlyingMeasure);
			} else if ("null".equals(s)) {
				operandToAppend = null;
			} else if ("*".equals(s) || "+".equals(s)) {
				List<Object> operands = new LinkedList<>(pendingOperands);
				pendingOperands.clear();

				// IAggregation aggregator = getAggregation(s);
				// operandToAppend = operands.stream().reduce(aggregator::aggregate).orElse(null);

				ICombination combination = getCombination(s);
				operandToAppend = combination.combine(operands);
			} else {
				log.warn("Not-managed: {}", s);
				operandToAppend = s;
			}
			pendingOperands.addLast(operandToAppend);
		}

		if (pendingOperands.size() == 1) {
			return pendingOperands.getFirst();
		} else {
			log.warn("Invalid number of leftover operands: {}", pendingOperands);
			return null;
		}
	}

	// TODO Rely on IOperatorsFactory
	protected ICombination getCombination(String operator) {
		if ("*".equals(operator) || ProductAggregation.KEY.equals(operator)) {
			return new ProductCombination();
		} else {
			throw new UnsupportedOperationException("Not-managed: " + operator);
		}
	}
	// protected IAggregation getAggregation(String operator) {
	// if("*".equals(operator) || ProductAggregator.KEY.equals(operator)) {
	// return new ProductAggregator();
	// } else {
	// throw new UnsupportedOperationException("Not-managed: " + operator);
	// }
	// }
}
