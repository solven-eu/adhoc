/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine.measure;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.Builder;
import lombok.Builder.Default;

/**
 * Make a {@link IMeasureQueryStep} given a {@link CubeQueryStep} and a {@link IHasUnderlyingMeasures}.
 * 
 * This is relevant to split `adhoc-measures` and `adhoc-dataframe` maven modules.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class MeasureQueryStepFactory implements IMeasureQueryStepFactory {
	@Default
	final IAdhocFactories factories = AdhocFactoriesUnsafe.factories;

	@Override
	public IMeasureQueryStep makeQueryStep(CubeQueryStep queryStep, IHasUnderlyingMeasures measure) {

		if (measure instanceof IMeasureQueryStepOwnFactory ownFactory) {
			return ownFactory.makeQueryStep(factories, queryStep);
		}

		String transformatorQueryStepClass = measure.queryStepClass();

		Class<?> queryStepClass;
		try {
			queryStepClass = Class.forName(transformatorQueryStepClass);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Ouch step=%s".formatted(queryStep), e);
		}
		Constructor<?>[] constructors = queryStepClass.getDeclaredConstructors();

		Optional<Constructor<?>> chosenConstructor = Stream.of(constructors)
				// Prefer matching a maximum number of parameters
				.sorted(Comparator.comparingLong(
						c -> Stream.of(c.getParameterTypes()).filter(p -> isAllowed(p, measure)).count()))
				// Require all parameters to be allowed
				.filter(c -> Stream.of(c.getParameterTypes()).allMatch(p -> isAllowed(p, measure)))
				.findFirst();

		if (chosenConstructor.isEmpty()) {
			throw new IllegalStateException(
					"Can not find a proper constructor for %s".formatted(queryStepClass.getName()));
		}

		Constructor<?> ctor = chosenConstructor.get();

		Object[] orderedArguments =
				Stream.of(ctor.getParameterTypes()).map(p -> bestArgument(p, queryStep, measure)).toArray();

		try {
			return (IMeasureQueryStep) ctor.newInstance(orderedArguments);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new IllegalArgumentException("Ouch", e);
		}
	}

	protected Object bestArgument(Class<?> clazz, CubeQueryStep queryStep, IHasUnderlyingMeasures measure) {
		if (clazz.isAssignableFrom(IAdhocFactories.class)) {
			return factories;
		} else if (clazz.isAssignableFrom(CubeQueryStep.class)) {
			return queryStep;
		} else if (clazz.isAssignableFrom(measure.getClass())) {
			return measure;
		} else {
			throw new IllegalStateException("Not managed parameter: %s".formatted(clazz));
		}
	}

	protected boolean isAllowed(Class<?> clazz, IHasUnderlyingMeasures measure) {
		Set<Class<?>> allowedParameters =
				ImmutableSet.of(measure.getClass(), CubeQueryStep.class, IAdhocFactories.class);

		return allowedParameters.stream().anyMatch(clazz::isAssignableFrom);
	}

}
