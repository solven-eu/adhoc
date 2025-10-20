package eu.solven.adhoc.query.filter.optimizer;

import eu.solven.adhoc.query.filter.stripper.IFilterStripperFactory;

/**
 * Helps re-using an {@link IFilterStripperFactory}.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IHasFilterStripperFactory {
	IFilterStripperFactory getFilterStripperFactory();
}
