package eu.solven.adhoc.api.v1;

import java.util.List;
import java.util.NavigableSet;

/**
 * A {@link List} of columns. Typically used by {@link IAdhocQuery}, or {@link IHolyCube}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAdhocGroupBy {
	IAdhocGroupBy GRAND_TOTAL = new GrandTotal();

	/**
	 * If true, there is not a single groupBy
	 * 
	 * @return
	 */
	default boolean isGrandTotal() {
		return getGroupedByColumns().isEmpty();
	}

	/**
	 * BEWARE How would this behave in case of rebucketing (e.g. word -> firstLetter)
	 * 
	 * @return the name of the groupBy when the input and output columns are identical.
	 */
	NavigableSet<String> getGroupedByColumns();
}