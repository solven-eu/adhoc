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
package eu.solven.adhoc.measure;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregator;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.combination.AdhocIdentity;
import eu.solven.adhoc.measure.combination.ExpressionCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.LinearDecomposition;
import eu.solven.adhoc.measure.step.IFilterEditor;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandardOperatorsFactory implements IOperatorsFactory {

	@Override
    public ICombination makeCombination(String key, Map<String, ?> options) {
        return switch (key) {
            case SumCombination.KEY: {
                yield new SumCombination();
            }
            case MaxCombination.KEY: {
                yield new MaxCombination();
            }
            case DivideCombination.KEY: {
                yield new DivideCombination(options);
            }
            case ExpressionCombination.KEY: {
                yield ExpressionCombination.parse(options);
            }
            default:
                yield defaultCombination(key, options);
        };
    }

	protected ICombination defaultCombination(String key, Map<String, ?> options) {
		Class<? extends ICombination> asClass;
		try {
			asClass = (Class<? extends ICombination>) Class.forName(key);

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
    public IAggregation makeAggregation(String key, Map<String, ?> options) {
        return switch (key) {
            case SumAggregation.KEY: {
                yield new SumAggregation();
            }
			case MaxAggregator.KEY: {
				yield new MaxAggregator();
			}
			case CountAggregation.KEY: {
				yield new CountAggregation();
			}
			case ExpressionAggregation.KEY: {
				yield new ExpressionAggregation();
			}
            default:
                yield defaultAggregation(key, options);
        };
    }

	protected IAggregation defaultAggregation(String key, Map<String, ?> options) {
		Class<? extends IAggregation> asClass;
		try {
			asClass = (Class<? extends IAggregation>) Class.forName(key);

		} catch (ClassNotFoundException e) {
			log.trace("No class matches %s".formatted(key));
			throw new IllegalArgumentException("Unexpected value: " + key);
		}

		try {
			try {
				Constructor<? extends IAggregation> constructorWithOptions = asClass.getConstructor(Map.class);
				return constructorWithOptions.newInstance(options);
			} catch (NoSuchMethodException e) {
				Constructor<? extends IAggregation> constructorWithNothing = asClass.getConstructor();
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
