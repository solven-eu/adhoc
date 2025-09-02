package eu.solven.adhoc.engine;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.step.CubeQueryStep;

/**
 * A Simpler {@link IQueryStepCache} based on a Map, with no discarding policy.
 * 
 * @author Benoit Lacelle
 */
public class MapQueryStepCache implements IQueryStepCache {
	final Map<CubeQueryStep, ISliceToValue> map = new ConcurrentHashMap<>();

	@Override
	public Optional<ISliceToValue> getValue(CubeQueryStep step) {
		return Optional.ofNullable(map.get(step));
	}

	@Override
	public void pushValues(Map<CubeQueryStep, ISliceToValue> queryStepToValues) {
		map.putAll(queryStepToValues);
	}

}