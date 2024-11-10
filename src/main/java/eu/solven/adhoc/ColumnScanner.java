package eu.solven.adhoc;

/**
 * Used with a {@link ITabularView} to iterate over its rows.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ColumnScanner<T> {
	void onRow(T coordinates, Object value);
}
