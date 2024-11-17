package eu.solven.adhoc;

import eu.solven.adhoc.storage.ValueConsumer;

/**
 * Used with a {@link ITabularView} to iterate over its rows.
 * 
 * @author Benoit Lacelle
 *
 */
public interface RowScanner<T> {
	ValueConsumer onKey(T coordinates);
}
