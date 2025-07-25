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
package eu.solven.adhoc.measure.operator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinCombination;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.LinearDecomposition;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.ProductAggregation;
import eu.solven.adhoc.measure.sum.ProductCombination;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.sum.SumNotNaNAggregation;
import eu.solven.adhoc.util.AdhocIdentity;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.Builder.Default;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A default {@link IOperatorFactory} wrapping core {@link IAggregation}, {@link ICombination}, {@link IDecomposition}
 * and {@link IFilterEditor}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuperBuilder
@NoArgsConstructor
public class StandardOperatorFactory implements IOperatorFactory {

	public static final String K_OPERATOR_FACTORY = "operatorFactory";

	// If null, this is defaulted to `this`
	@Default
	IOperatorFactory rootOperatorFactory = null;

	protected IOperatorFactory getRootOperatorFactory() {
		if (rootOperatorFactory == null) {
			return this;
		} else {
			return rootOperatorFactory;
		}
	}

	@Override
	public IAggregation makeAggregation(String key, Map<String, ?> options) {
		return switch (key) {
		case SumAggregation.KEY:
			yield new SumAggregation();
		case SumNotNaNAggregation.KEY:
			yield new SumNotNaNAggregation();
		case MaxAggregation.KEY:
			yield MaxAggregation.builder().build();
		case MinAggregation.KEY:
			yield MinAggregation.builder().build();
		case CountAggregation.KEY:
			yield new CountAggregation();
		case ExpressionAggregation.KEY:
			yield new ExpressionAggregation();
		case EmptyAggregation.KEY:
			yield new EmptyAggregation();
		case RankAggregation.KEY:
			yield RankAggregation.make(options);
		case AvgAggregation.KEY:
			yield new AvgAggregation();
		case CoalesceAggregation.KEY:
			yield new CoalesceAggregation(options);
		default:
			yield defaultAggregation(key, options);
		};
	}

	protected IAggregation defaultAggregation(String className, Map<String, ?> options) {
		return makeWithMapOrEmpty(IAggregation.class, className, options);
	}

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		if (SumAggregation.isSum(key)) {
			return new SumCombination();
		} else if (ProductAggregation.isProduct(key)) {
			return new ProductCombination();
		} else if (DivideCombination.isDivide(key)) {
			return new DivideCombination();
		} else if (SubstractionCombination.isSubstraction(key)) {
			return new SubstractionCombination();
		}

		Map<String, ?> enrichedOptions = enrichOptions(options);

		return switch (key) {
		case MaxCombination.KEY: {
			yield new MaxCombination();
		}
		case MinCombination.KEY: {
			yield new MaxCombination();
		}
		case EvaluatedExpressionCombination.KEY: {
			try {
				Class.forName("com.ezylang.evalex.Expression");
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException("com.ezylang:EvalEx is seemingly not present in the class-loaded."
						+ " It is an `<optional>true</optional>` maven dependency you need to activate manually", e);
			}
			yield EvaluatedExpressionCombination.parse(enrichedOptions);
		}
		case CoalesceCombination.KEY: {
			yield new CoalesceCombination();
		}
		default:

			yield defaultCombination(key, enrichedOptions);
		};
	}

	protected Map<String, Object> enrichOptions(Map<String, ?> options) {
		Map<String, Object> enriched = new LinkedHashMap<>(options);

		// Some operators needs to create more operators (e.g. ReversePolishCombination)
		enriched.put(K_OPERATOR_FACTORY, getRootOperatorFactory());

		return enriched;
	}

	protected ICombination defaultCombination(String className, Map<String, ?> options) {
		return makeWithMapOrEmpty(ICombination.class, className, options);
	}

	protected <T> T makeWithMapOrEmpty(Class<? extends T> parentClazz, String className, Map<String, ?> options) {
		// TODO Start searching for a static method named `make`
		Class<? extends T> asClass;
		try {
			asClass = (Class<? extends T>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			log.trace("No class matches %s".formatted(className));
			throw new IllegalArgumentException("Unexpected value: " + className, e);
		}

		if (!parentClazz.isAssignableFrom(asClass)) {
			throw new IllegalArgumentException("class=`%s` does not extends %s".formatted(className, parentClazz));
		}

		try {
			try {
				Constructor<? extends T> constructorWithOptions = asClass.getConstructor(Map.class);
				return constructorWithOptions.newInstance(options);
			} catch (NoSuchMethodException e) {
				log.trace("No `<Map>` constructor. Checking for an empty constructor.");
				Constructor<? extends T> constructorWithNothing = asClass.getConstructor();
				return constructorWithNothing.newInstance();
			}
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException
				| NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		return switch (key) {
		case AdhocIdentity.KEY: {
			yield new AdhocIdentity();
		}
		case LinearDecomposition.KEY: {
			yield new LinearDecomposition(options);
		}
		default:

			yield defaultDecomposition(key, options);
		};

	}

	protected IDecomposition defaultDecomposition(String className, Map<String, ?> options) {
		return makeWithMapOrEmpty(IDecomposition.class, className, options);
	}

	@Override
	public IFilterEditor makeEditor(String key, Map<String, ?> options) {
		return switch (key) {
		case AdhocIdentity.KEY: {
			yield f -> f;
		}
		case SimpleFilterEditor.KEY: {
			Map<String, Object> columnToValue = MapPathGet.getRequiredMap(options, SimpleFilterEditor.P_SHIFTED);

			yield SimpleFilterEditor.builder().columnToValues(columnToValue).build();
		}
		default:

			yield defaultEditor(key, options);
		};

	}

	protected IFilterEditor defaultEditor(String className, Map<String, ?> options) {
		return makeWithMapOrEmpty(IFilterEditor.class, className, options);
	}

	@Override
	public IOperatorFactory withRoot(IOperatorFactory rootOperatorFactory) {
		return StandardOperatorFactory.builder().rootOperatorFactory(rootOperatorFactory).build();
	}

}
