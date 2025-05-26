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
package eu.solven.adhoc.measure.combination;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import com.google.common.base.CharMatcher;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements Polish-notations to express formulas. See https://en.wikipedia.org/wiki/Polish_notation.
 *
 */
// TODO Enable shifting some column
@Slf4j
public class ReversePolishCombination implements ICombination, IHasSanityChecks {

	public static final String K_NOTATION = "notation";

	final String notation;
	// Strict ReversePolishNotation only consumes 2 operands per operator
	// But one may feel simpler to provide things like `a,b,c,+`
	final boolean twoOperandsPerOperator;
	// If there is no underlying, should this return null?
	// e.g. `aggregatedValue[m]+100` could return `null` or `100` if `m` is null
	final boolean nullIfNotASingleUnderlying;
	final Map<String, Integer> underlyingMeasuresToIndex = new LinkedHashMap<>();

	final IOperatorsFactory operatorsFactory;

	public ReversePolishCombination(Map<String, ?> options) {
		// e.g. `aggregatedValue[someMeasure],double[10000],*`
		notation = parseNotation(options);

		// Default is false (sum all pending operands), to follow ActiveViam convention
		twoOperandsPerOperator = MapPathGet.<Boolean>getOptionalAs(options, "twoOperandsPerOperator").orElse(false);

		// Default is true (no underlying -> `null`), to follow ActiveViam convention
		nullIfNotASingleUnderlying =
				MapPathGet.<Boolean>getOptionalAs(options, "nullIfNotASingleUnderlying").orElse(true);

		parseUnderlyingMeasures(notation).forEach(underlyingMeasure -> underlyingMeasuresToIndex.put(underlyingMeasure,
				underlyingMeasuresToIndex.size()));

		operatorsFactory = MapPathGet.<IOperatorsFactory>getOptionalAs(options, "operatorsFactory")
				.orElseGet(StandardOperatorsFactory::new);
	}

	/**
	 *
	 * @return the {@link Set} of underlying measures, in their order of appearance.
	 */
	public static Collection<String> parseUnderlyingMeasures(String formula) {
		return Pattern.compile(EvaluatedExpressionCombination.P_UNDERLYINGS + "\\[([^\\]]+)\\]")
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
	protected String parseNotation(Map<String, ?> options) {
		// e.g. `aggregatedValue[someMeasureName],double[10000],*`
		String formula = MapPathGet.getRequiredString(options, K_NOTATION);

		// TODO Throw early if the formula is invalid

		return formula;
	}

	@Override
	public void checkSanity() {
		// TODO Auto-generated method stub

	}

	// https://github.com/maximfersko/Reverse-Polish-Notation-Library/blob/main/src/main/java/com/fersko/reversePolishNotation/ReversePolishNotation.java
	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		// Regex capturing `(x)` where `x` is a ReversePolishNotation
		Pattern subFormulaRegex = Pattern.compile("\\(([^\\(\\)]+)\\)");

		// BEWARE If the formula is constant, this will remain false
		AtomicBoolean oneUnderlyingIsNotNull = new AtomicBoolean();
		String replacedFormula = notation;

		do {
			String nextFormula = subFormulaRegex.matcher(replacedFormula).replaceAll(mr -> {
				String subFormula = mr.group(1);
				Object evaluatedSubFormula =
						evaluateReversePolish(subFormula, slice, underlyingValues, oneUnderlyingIsNotNull);
				log.debug("subFormula={} evaluated into {}", subFormula, evaluatedSubFormula);

				if (evaluatedSubFormula == null) {
					return "null";
				}

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

		return evaluateReversePolish(replacedFormula, slice, underlyingValues, oneUnderlyingIsNotNull);
	}

	protected Object evaluateReversePolish(String formula,
			ISliceWithStep slice,
			List<?> underlyingValues,
			AtomicBoolean oneUnderlyingIsNotNull) {
		List<Object> pendingOperands = new LinkedList<>();
		String[] elements = splitFormula(formula);

		if (underlyingValues.size() < underlyingMeasuresToIndex.size()) {
			throw new IllegalArgumentException("Received %s underlyings while formula refers to %s aggregatedValue"
					.formatted(underlyingValues.size(), underlyingMeasuresToIndex.size()));
		}

		for (int i = 0; i < elements.length; i++) {
			String s = elements[i];

			Object operandToAppend;

			// OperandAggregatedValue.PLUGIN_TYPE
			if (s.startsWith(EvaluatedExpressionCombination.P_UNDERLYINGS + "[") && s.endsWith("]")) {
				String underlyingMeasure =
						s.substring(EvaluatedExpressionCombination.P_UNDERLYINGS.length() + "[".length(),
								s.length() - "]".length());
				int measureIndex = underlyingMeasuresToIndex.get(underlyingMeasure);
				operandToAppend = underlyingValues.get(measureIndex);
				if (operandToAppend != null) {
					oneUnderlyingIsNotNull.set(true);
				}
			} else if (s.startsWith("double[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("double[".length(), s.length() - "]".length());
				operandToAppend = Double.parseDouble(underlyingMeasure);
			} else if (s.startsWith("int[") && s.endsWith("]")) {
				String underlyingMeasure = s.substring("int[".length(), s.length() - "]".length());
				operandToAppend = Integer.parseInt(underlyingMeasure);
			} else if ("null".equals(s)) {
				operandToAppend = null;
			} else if (s.matches("-?[0-9]+")) {
				operandToAppend = Long.parseLong(s);
			} else if (s.matches("-?[0-9]+.[0-9]+([Ee][+-]?[0-9]+)?")) {
				// https://stackoverflow.com/questions/10516967/regexp-for-a-double
				operandToAppend = Double.parseDouble(s);
			} else {
				operandToAppend = onOperator(slice, pendingOperands, s);
			}
			pendingOperands.addLast(operandToAppend);
		}

		if (pendingOperands.size() == 1) {
			if (nullIfNotASingleUnderlying && !oneUnderlyingIsNotNull.get()) {
				return null;
			}
			return pendingOperands.getFirst();
		} else {
			log.warn("Invalid number of leftover operands: {} in formula=`{}`", pendingOperands, formula);
			return null;
		}
	}

	protected String[] splitFormula(String formula) {
		String tokens = ",";
		return formula.split(tokens);
	}

	protected Object onOperator(ISliceWithStep slice, List<Object> pendingOperands, String s) {
		// TODO We may also accept any IAggregation
		ICombination combination = getCombination(s);

		List<Object> operands;

		if (twoOperandsPerOperator || combination instanceof IHasTwoOperands) {
			operands = new LinkedList<>();
			// Add at the beginning to reverse the stack
			operands.add(0, pendingOperands.removeLast());
			operands.add(0, pendingOperands.removeLast());
		} else {
			// No need to reverse the stack for these operators
			operands = new LinkedList<>(pendingOperands);
			pendingOperands.clear();
		}

		return combination.combine(slice, operands);
	}

	protected ICombination getCombination(String operator) {
		return operatorsFactory.makeCombination(operator, Map.of());
	}
}
