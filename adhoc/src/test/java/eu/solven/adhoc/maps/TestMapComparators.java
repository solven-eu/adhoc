package eu.solven.adhoc.maps;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;

import org.junit.jupiter.api.Test;

import eu.solven.adhoc.coordinate.MapComparators;

public class TestMapComparators {
	@Test
	public void testGenerics() {
		Comparator<Map<?, ?>> comparatorWildcardWildcard = MapComparators.mapComparator();
		Comparator<Map<String, ?>> comparatorStringWildcard = MapComparators.mapComparator();
		Comparator<Map<String, ?>> comparatorStringWildcardWithExplicit =
				MapComparators.<Map<String, ?>>mapComparator();
	}

	@Test
	public void testGenerics_mapIntonavigable() {
		Comparator<NavigableMap<?, ?>> comparatorWildcardWildcard = MapComparators.mapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcard = MapComparators.mapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcardWithExplicit =
				MapComparators.<NavigableMap<String, ?>>mapComparator();
	}

	@Test
	public void testGenerics_navigableIntoNavigable() {
		Comparator<NavigableMap<?, ?>> comparatorWildcardWildcard = MapComparators.navigableMapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcard = MapComparators.navigableMapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcardWithExplicit =
				MapComparators.<NavigableMap<String, ?>>navigableMapComparator();
	}
}
