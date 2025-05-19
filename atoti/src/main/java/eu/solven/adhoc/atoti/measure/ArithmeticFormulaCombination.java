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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.CharMatcher;
import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.ProductAggregation;
import eu.solven.adhoc.measure.sum.ProductCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements Polish-notations to express formulas. See https://en.wikipedia.org/wiki/Polish_notation.
 *
 * Can be used as alternative to {@link com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor}
 */
@Slf4j
public class ArithmeticFormulaCombination implements ICombination {
	final String formula;
	// Strict ReversePolishNotation only consumes 2 operands per operator
	// But one may feel simpler to provide things like `a,b,c,+`
	final boolean twoOperandsPerOperator;
	final Map<String, Integer> underlyingMeasuresToIndex = new LinkedHashMap<>();

	public ArithmeticFormulaCombination(Map<String, ?> options) {
		formula = parseFormula(options);

		// Default is false, to follow ActiveViam convention
		twoOperandsPerOperator = MapPathGet.<Boolean>getOptionalAs(options, "twoOperandsPerOperator").orElse(false);

		Pattern.compile("aggregatedValue\\[([^\\]]+)\\]")
				.matcher(formula)
				.results()
				.map(mr -> mr.group(1))
				.forEach(underlyingMeasure -> underlyingMeasuresToIndex.put(underlyingMeasure,
						underlyingMeasuresToIndex.size()));
		// String[] elements = formula.split(",");
		//
		// Stream.of(elements)
		// .filter(s -> s.startsWith("aggregatedValue[") && s.endsWith("]"))
		// .map(s -> s.substring("aggregatedValue[".length(), s.length() - "]".length()))
		// .forEach(underlyingMeasure -> underlyingMeasuresToIndex.put(underlyingMeasure,
		// underlyingMeasuresToIndex.size()));
		System.out.println(underlyingMeasuresToIndex);
	}

	public List<String> getUnderlyingMeasures() {
		return underlyingMeasuresToIndex.keySet().stream().toList();
	}

	// BEWARE This is called from the constructor
	protected String parseFormula(Map<String, ?> options) {
		// e.g. `aggregatedValue[CDS Composite Spread5Y],double[10000],*`
		String formula = MapPathGet.getRequiredString(options, ArithmeticFormulaPostProcessor.FORMULA_PROPERTY);

		if (formula.startsWith("(") && formula.endsWith(")")) {
			// https://en.wikipedia.org/wiki/Reverse_Polish_notation
			// Reverse Polish Notation should not accept `()`, as it does not need them. ActiveViam `Reverse Polish
			// Notation` needs some specification.
			if (CharMatcher.is('(').countIn(formula) == 1 && CharMatcher.is(')').countIn(formula) == 1) {
				// Drop surrounding parenthesis
				formula = formula.substring(1, formula.length() - 1);
			} else {
				throw new IllegalArgumentException(
						"ReversePolishNotation does not accept (nor need) parenthesis. formula=`%s`"
								.formatted(formula));
			}
		}

		return formula;
	}

	// https://github.com/maximfersko/Reverse-Polish-Notation-Library/blob/main/src/main/java/com/fersko/reversePolishNotation/ReversePolishNotation.java
	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		List<Object> pendingOperands = new LinkedList<>();

		String tokens = ",";
		String[] elements = formula.split(tokens);

		// Stack<String> pendingOperands = new Stack<>();

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
			} else if ("*".equals(s) || "+".equals(s) || "/".equals(s)) {
				List<Object> operands;

				if (twoOperandsPerOperator || "/".equals(s)) {
					operands = new LinkedList<>();
					// Add at the beginning to reverse the stack
					operands.add(0, pendingOperands.removeLast());
					operands.add(0, pendingOperands.removeLast());
				} else {
					// No need to reverse the stack for these operators
					operands = new LinkedList<>(pendingOperands);
					pendingOperands.clear();
				}

				ICombination combination = getCombination(s);
				operandToAppend = combination.combine(slice, operands);
			} else if (s.matches("-?[0-9]+")) {
				operandToAppend = Integer.parseInt(s);
			} else {
				log.warn("Not-managed: {}", s);
				operandToAppend = s;
			}
			pendingOperands.addLast(operandToAppend);
		}

		if (pendingOperands.size() == 1) {
			return pendingOperands.getFirst();
		} else {
			log.warn("Invalid number of leftover operands: {} in formula=`{}`", pendingOperands, formula);
			return null;
		}
	}

	// TODO Rely on IOperatorsFactory
	protected ICombination getCombination(String operator) {
		if (ProductAggregation.isProduct(operator)) {
			return new ProductCombination();
		} else if (SumAggregation.isSum(operator)) {
			return new SumCombination();
		} else if (DivideCombination.isDivide(operator)) {
			return new DivideCombination();
		} else {
			throw new UnsupportedOperationException("Not-managed: " + operator);
		}
	}
}
