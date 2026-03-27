package eu.solven.adhoc.engine.step;

import java.util.Map;

/**
 * Gives access to a transverse cache. It is transverse in the sense it is shared through a whole CubeQuery.
 * 
 * @author Benoit Lacelle
 */
public interface IHasTransverseCache {

	Map<Object, Object> getTransverseCache();

}
