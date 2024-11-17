package eu.solven.adhoc.coordinate;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Various {@link Comparator} for {@link Map}.
 * 
 * @author Benoit Lacelle
 *
 */
public class MapComparators {
	public static <M extends NavigableMap> Comparator<M> navigableMapComparator() {
		return (Comparator<M>) NavigableMapComparator.INSTANCE;
	}

	/**
	 * BEWARE This {@link Comparator} is quite slow, as it may make two {@link HashMap} on each comparison.
	 * 
	 * @return
	 */
	public static <M extends Map> Comparator<M> mapComparator() {
		// Various casts are here to help javac doing its job
		return Comparator.comparing(map -> {
			NavigableMap asNavigableMap = MapComparators.asNavigableMap((Map) map);
			return (Map) asNavigableMap;
		}, (Comparator) NavigableMapComparator.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	private static <K, V> NavigableMap<K, V> asNavigableMap(Map<K, V> map) {
		if (map instanceof NavigableMap<?, ?> navigableMap) {
			return (NavigableMap<K, V>) navigableMap;
		} else {
			return new TreeMap<>(map);
		}
	}
}
