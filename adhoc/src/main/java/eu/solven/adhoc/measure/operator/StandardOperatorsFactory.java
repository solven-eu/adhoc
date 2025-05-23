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
import java.util.Map;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinCombination;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.combination.AdhocIdentity;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.LinearDecomposition;
import eu.solven.adhoc.measure.sum.*;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandardOperatorsFactory implements IOperatorsFactory {

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
		default:
			yield defaultAggregation(key, options);
		};
	}

	protected IAggregation defaultAggregation(String className, Map<String, ?> options) {
		return makeWithMapOrEmpty(IAggregation.class, className, options);
	}

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return switch (key) {
		case SumCombination.KEY: {
			yield new SumCombination();
		}
		case DivideCombination.KEY: {
			yield new DivideCombination(options);
		}
		case ProductCombination.KEY: {
			yield new ProductCombination(options);
		}
		case SubstractionCombination.KEY: {
			yield new SubstractionCombination();
		}
		case MaxCombination.KEY: {
			yield new MaxCombination();
		}
		case MinCombination.KEY: {
			yield new MaxCombination();
		}
		case EvaluatedExpressionCombination.KEY: {
			try {
				Class.forName("com.ezylang.evalex.Expression");
			} catch (ClassNotFoundException ex) {
				throw new IllegalStateException("com.ezylang:EvalEx is seemingly not present in the class-loaded."
						+ " It is an optional maven dependency you need to activate manually");
			}
			yield EvaluatedExpressionCombination.parse(options);
		}
		case FindFirstCombination.KEY: {
			yield new FindFirstCombination();
		}
		default:

			yield defaultCombination(key, options);
		};

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
			throw new IllegalArgumentException("Unexpected value: " + className);
		}

		if (!parentClazz.isAssignableFrom(asClass)) {
			throw new IllegalArgumentException("class=`%s` does not extends %s".formatted(className, parentClazz));
		}

		try {
			try {
				Constructor<? extends T> constructorWithOptions = asClass.getConstructor(Map.class);
				return constructorWithOptions.newInstance(options);
			} catch (NoSuchMethodException e) {
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

	protected IDecomposition defaultDecomposition(String key, Map<String, ?> options) {
		Class<? extends IDecomposition> asClass;
		try {
			asClass = (Class<? extends IDecomposition>) Class.forName(key);

		} catch (ClassNotFoundException e) {
			log.trace("No class matches %s".formatted(key));
			throw new IllegalArgumentException("Unexpected value: " + key);
		}

		try {
			return asClass.getConstructor(Map.class).newInstance(options);
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException
				| NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
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

	protected IFilterEditor defaultEditor(String key, Map<String, ?> options) {
		Class<? extends IFilterEditor> asClass;
		try {
			asClass = (Class<? extends IFilterEditor>) Class.forName(key);
		} catch (ClassNotFoundException e) {
			log.trace("No class matches %s".formatted(key));
			throw new IllegalArgumentException("Unexpected value: " + key);
		}

		try {
			return asClass.getConstructor(Map.class).newInstance(options);
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException
				| NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

}
