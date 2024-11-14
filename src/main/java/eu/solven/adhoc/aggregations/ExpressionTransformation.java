package eu.solven.adhoc.aggregations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ezylang.evalex.EvaluationException;
import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.parser.ParseException;

import eu.solven.pepper.collection.PepperMapHelper;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Enable expression-based {@link ITransformation}
 * 
 * @author Benoit Lacelle
 * @see https://github.com/ezylang/EvalEx
 */
@RequiredArgsConstructor
public class ExpressionTransformation implements ITransformation {

	public static final String KEY = "EXPRESSION";

	@NonNull
	final String expression;
	@NonNull
	final List<String> underlyingNames;

	@Override
	public Object transform(List<?> underlyingValues) {
		Expression exp = new Expression(expression);

		EvaluationValue result;
		try {
			Map<String, Object> nameToValue = new HashMap<>();

			for (int i = 0; i < Math.min(underlyingNames.size(), underlyingValues.size()); i++) {
				nameToValue.put(underlyingNames.get(i), underlyingValues.get(i));
			}

			result = exp.withValues(nameToValue).evaluate();

		} catch (EvaluationException | ParseException e) {
			throw new IllegalArgumentException(e);
		}

		return result.getValue();
	}

	public static ExpressionTransformation parse(Map<String, ?> options) {
		String expression = PepperMapHelper.getRequiredString(options, "expression");
		List<String> underlyingIndexToName = PepperMapHelper.getRequiredAs(options, "underlyingNames");
		return new ExpressionTransformation(expression, underlyingIndexToName);
	}

}
