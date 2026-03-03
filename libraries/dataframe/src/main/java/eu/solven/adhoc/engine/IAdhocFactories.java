package eu.solven.adhoc.engine;

import eu.solven.adhoc.engine.tabular.optimizer.IFilterOptimizerFactory;
import eu.solven.adhoc.map.factory.ISliceFactoryFactory;
import eu.solven.adhoc.measure.model.ITransformatorQueryStepFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.query.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.util.IStopwatchFactory;

public interface IAdhocFactories {

	IOperatorFactory getOperatorFactory();

	IColumnFactory getColumnFactory();

	ISliceFactoryFactory getSliceFactoryFactory();

	IFilterOptimizerFactory getFilterOptimizerFactory();

	IFilterStripperFactory getFilterStripperFactory();

	IStopwatchFactory getStopwatchFactory();

	ITransformatorQueryStepFactory getTransformatorFactory();
}
