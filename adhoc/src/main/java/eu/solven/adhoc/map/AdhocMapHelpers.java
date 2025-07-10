package eu.solven.adhoc.map;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.experimental.UtilityClass;

/**
 * Helper methods related to {@link Map} in the context of Adhoc.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocMapHelpers {

	/**
	 * 
	 * @param map
	 * @return an immutable copy of the input, which may or may not be an {@link AdhocMap}
	 */
	public static Map<String, Object> immutableCopyOf(Map<String, ?> map) {
		if (map instanceof IAdhocMap adhocMap) {
			// For performance, we expect to be generally in this branch
			return adhocMap;
		} else {
			return ImmutableMap.copyOf(map);
		}
	}

}
