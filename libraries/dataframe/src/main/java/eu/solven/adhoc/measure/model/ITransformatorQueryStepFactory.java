package eu.solven.adhoc.measure.model;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;

public interface ITransformatorQueryStepFactory {

	default ITransformatorQueryStep makeTransformatorQueryStep(CubeQueryStep queryStep,
			IHasUnderlyingMeasures measureWithUnderlyings) {
		IAdhocFactories factories = AdhocFactoriesUnsafe.factories;

		if (measureWithUnderlyings instanceof ITransformatorQueryStepOwnFactory ownFactory) {
			return ownFactory.wrapNode(factories, queryStep);
		}

		String transformatorQueryStepClass = measureWithUnderlyings.wrapNode();

		Constructor<?> ctor;
		try {
			ctor = Class.forName(transformatorQueryStepClass)
					.getConstructor(measureWithUnderlyings.getClass(), IAdhocFactories.class, CubeQueryStep.class);
			return (ITransformatorQueryStep) ctor.newInstance(measureWithUnderlyings, factories, queryStep);
		} catch (NoSuchMethodException | ClassNotFoundException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new IllegalArgumentException("Ouch", e);
		}

	}

	interface ITransformatorQueryStepOwnFactory extends IHasUnderlyingMeasures {
		ITransformatorQueryStep wrapNode(IAdhocFactories factories, CubeQueryStep queryStep);
	}

	static ITransformatorQueryStepFactory standard() {
		return new ITransformatorQueryStepFactory() {
		};
	}
}
