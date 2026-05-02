package eu.solven.adhoc.atoti.measure;

import java.util.concurrent.ConcurrentMap;

import com.quartetfs.biz.pivot.postprocessing.impl.BasicPostProcessor;
import com.quartetfs.biz.pivot.query.IQueryCache;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;

/**
 * Helps writing an {@link BasicPostProcessor} as an {@link ICombination}
 * 
 * @author Benoit Lacelle
 */
public abstract class AAtotiCombination implements ICombination {
	/**
	 * {@link IQueryCache} is essentially a {@link ConcurrentMap} shared through the whole query lifecycle.
	 * 
	 * @param slice
	 * @return
	 */
	protected ConcurrentMap<Object, Object> getQueryCache(ISliceWithStep slice) {
		return slice.getQueryStep().getTransverseCache();
	}
}
