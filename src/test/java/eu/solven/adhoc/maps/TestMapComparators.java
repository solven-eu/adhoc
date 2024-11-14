package eu.solven.adhoc.maps;

import java.util.Comparator;
import java.util.Map;

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
}
