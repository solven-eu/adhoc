package eu.solven.adhoc;

import java.util.Map;

/**
 * Used with a {@link ITabularView} to iterate over its rows.
 * 
 * @author Benoit Lacelle
 *
 */
public interface RowScanner {
	void onRow(Map<String, ?> coordinates, Map<String, ?> values);
}
