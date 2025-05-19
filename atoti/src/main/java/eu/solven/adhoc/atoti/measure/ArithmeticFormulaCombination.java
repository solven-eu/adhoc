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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements Polish-notations to express formulas. See https://en.wikipedia.org/wiki/Polish_notation.
 *
 * Can be used as alternative to {@link com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor}
 */
// TODO Wrap parenthesis in subFormula (ActiveViam extension of ReversePolishNotation)
// TODO Enable reading along members
@Slf4j
public class ArithmeticFormulaCombination implements ICombination {
	final String formula;
	// Strict ReversePolishNotation only consumes 2 operands per operator
	// But one may feel simpler to provide things like `a,b,c,+`
	final boolean twoOperandsPerOperator;
	// If there is no underlying, should this return null?
	// e.g. `aggregatedValue[m]+100` could return `null` or `100` if `m` is null
	final boolean nullIfNotASingleUnderlying;
	final Map<String, Integer> underlyingMeasuresToIndex = new LinkedHashMap<>();

	public ArithmeticFormulaCombination(Map<String, ?> options) {
		// e.g. `aggregatedValue[CDS Composite Spread5Y],double[10000],*`
		formula = parseFormula(options);

		// Default is false (sum all pending operands), to follow ActiveViam convention
		twoOperandsPerOperator = MapPathGet.<Boolean>getOptionalAs(options, "twoOperandsPerOperator").orElse(false);

		// Default is true (no underlying -> `null`), to follow ActiveViam convention
		nullIfNotASingleUnderlying =
				MapPathGet.<Boolean>getOptionalAs(options, "nullIfNotASingleUnderlying").orElse(true);

		parseUnderlyingMeasures(formula).forEach(underlyingMeasure -> underlyingMeasuresToIndex.put(underlyingMeasure,
				underlyingMeasuresToIndex.size()));
	}

	/**
	 *
	 * @return the {@link Set} of underlying measures, in their order of appearance.
	 */
	public static Collection<String> parseUnderlyingMeasures(String formula) {
		return Pattern.compile("aggregatedValue\\[([^\\]]+)\\]")
				.matcher(formula)
				.results()
				.map(mr -> mr.group(1))
				.toList();
	}

	/**
	 *
	 * @return the {@link Set} of underlying measures, in their order of appearance.
	 */
	public List<String> getUnderlyingMeasures() {
		return underlyingMeasuresToIndex.keySet().stream().toList();
	}

	// BEWARE This is called from the constructor
	protected String parseFormula(Map<String, ?> options) {
		// e.g. `aggregatedValue[someMeasureName],double[10000],*`
		String formula = MapPathGet.getRequiredString(options, ArithmeticFormulaPostProcessor.FORMULA_PROPERTY);

		if (formula.startsWith("(") && formula.endsWith(")")) {
			// https://en.wikipedia.org/wiki/Reverse_Polish_notation
			// Reverse Polish Notation should not accept `()`, as it does not need them. ActiveViam `Reverse Polish
			// Notation` needs some specification.
			if (CharMatcher.is('(').countIn(formula) == 1 && CharMatcher.is(')').countIn(formula) == 1) {
				// Drop surrounding parenthesis
				formula = formula.substring(1, formula.length() - 1);
			} else {
				// throw new IllegalArgumentException(
				// "ReversePolishNotation does not accept (nor need) parenthesis. formula=`%s`"
				// .formatted(formula));
			}
		}

		return formula;
	}

	// https://github.com/maximfersko/Reverse-Polish-Notation-Library/blob/main/src/main/java/com/fersko/reversePolishNotation/ReversePolishNotation.java
	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		// Regex capturing `(x)` where `x` is a ReversePolishNotation
		Pattern subFormulaRegex = Pattern.compile("\\(([^\\(\\)]+)\\)");

		String replacedFormula = formula;

		do {
			String nextFormula = subFormulaRegex.matcher(replacedFormula).replaceAll(mr -> {
				String subFormula = mr.group(1);
				Object evaluatedSubFormula = evaluateReversePolish(subFormula, slice, underlyingValues);
				log.debug("subFormula={} evaluated into {}", subFormula, evaluatedSubFormula);
				String evaluatedToString = evaluatedSubFormula.toString();

				if (CharMatcher.anyOf("()").matchesAnyOf(evaluatedToString)) {
					// TODO For now, we forbid such case which could lead to infinite loops
					throw new NotYetImplementedException("Evaluated subFormula must not generate parenthesis");
				}

				return evaluatedToString;
			});

			if (nextFormula.equals(replacedFormula)) {
				// no more subFormulas
				break;
			} else {
				// continue replacing subFormulas
				replacedFormula = nextFormula;
			}
		} while (true);

		return evaluateReversePolish(replacedFormula, slice, underlyingValues);
	}

	private Object evaluateReversePolish(String formula, ISliceWithStep slice, List<?> underlyingValues) {
		List<Object> pendingOperands = new LinkedList<>();
		String tokens = ",";
		String[] elements = formula.split(tokens);

		// If the formula is constant, this will remain false
		boolean oneUnderlyingIsNotNull = false;

		for (int i = 0; i < elements.length; i++) {
			String s = elements[i];

			Object operandToAppend;

			// OperandAggregatedValue.PLUGIN_TYPE
			if (s.startsWith("aggregatedValue[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("aggregatedValue[".length(), s.length() - "]".length());
				int measureIndex = underlyingMeasuresToIndex.get(underlyingMeasure);
				operandToAppend = underlyingValues.get(measureIndex);
				if (operandToAppend != null) {
					oneUnderlyingIsNotNull = true;
				}
			} else if (s.startsWith("double[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("double[".length(), s.length() - "]".length());
				operandToAppend = Double.parseDouble(underlyingMeasure);
			} else if (s.startsWith("int[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("int[".length(), s.length() - "]".length());
				operandToAppend = Integer.parseInt(underlyingMeasure);
			} else if ("null".equals(s)) {
				operandToAppend = null;
			} else if (ProductAggregation.isProduct(s) || SumAggregation.isSum(s) || DivideCombination.isDivide(s)) {
				List<Object> operands;

				if (twoOperandsPerOperator || DivideCombination.isDivide(s)) {
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
				operandToAppend = Long.parseLong(s);
			} else if (s.matches("-?[0-9]+.[0-9]+")) {
				operandToAppend = Double.parseDouble(s);
			} else {
				log.warn("Not-managed: {}", s);
				operandToAppend = s;
			}
			pendingOperands.addLast(operandToAppend);
		}

		if (pendingOperands.size() == 1) {
			if (nullIfNotASingleUnderlying && !oneUnderlyingIsNotNull) {
				return null;
			}
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
