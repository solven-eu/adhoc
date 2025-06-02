package eu.solven.adhoc.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import eu.solven.pepper.mappath.MapPathGet;
import lombok.experimental.UtilityClass;

/**
 * Similar to {@link MapPathGet}, with some Adhoc customizations.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocMapPathGet {

	public static Map<String, ?> getRequiredMap(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredMap(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public static String getRequiredString(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredString(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public static Object getRequiredAs(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredAs(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public static List<?> getRequiredList(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredAs(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	protected static <T> T onIllegalGet(Map<String, ?> map, String key, IllegalArgumentException e) {
		if (map.isEmpty()) {
			throw new IllegalArgumentException("input map is empty while looking for %s".formatted(key));
		} else if (e.getMessage().contains("(key not present)")) {
			String minimizingDistance = minimizingDistance(map.keySet(), key);

			if (SmileEditDistance.levenshtein(minimizingDistance, key) <= 2) {
				throw new IllegalArgumentException(
						"Did you mean `%s` instead of `%s`".formatted(minimizingDistance, key),
						e);
			} else {
				// It seems we're rather missing the input than having a typo
				throw e;
			}
		} else {
			throw e;
		}
	}

	/**
	 *
	 * @param options
	 * @param key
	 * @return the option minimizing its distance to the requested key.
	 */
	public static String minimizingDistance(Collection<String> options, String key) {
		return options.stream().min(Comparator.comparing(s -> SmileEditDistance.levenshtein(s, key))).orElse("?");
	}
}
