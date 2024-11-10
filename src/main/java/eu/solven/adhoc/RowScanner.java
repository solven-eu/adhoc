package eu.solven.adhoc;

import java.util.Map;

import eu.solven.adhoc.storage.ValueConsumer;

/**
 * Used with a {@link ITabularView} to iterate over its rows.
 * 
 * @author Benoit Lacelle
 *
 */
public interface RowScanner<T> {
	void onRow(T coordinates, Map<String, ?> values);

	ValueConsumer onKey(T coordinates);
}
