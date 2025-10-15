package eu.solven.adhoc.engine.step;

import java.util.Map;

import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.experimental.UtilityClass;

/**
 * Helps working with the {@link CubeQueryStep} transverse Cache.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class TransverseCacheHelper {
	public static IFilterOptimizer getFilterOptimizer(CubeQueryStep step) {
		Map<Object, Object> transverseCache = step.getTransverseCache();
		return (IFilterOptimizer) transverseCache.get(CubeQueryStep.KEY_FILTER_OPTIMIZER);
	}

}
