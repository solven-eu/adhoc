package eu.solven.adhoc.api.v1;

import java.util.List;

/**
 * A {@link List} of columns. Typically used by {@link IAdhocQuery}, or {@link IHolyCube}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasGroupBy {
	IHasGroupBy GRAND_TOTAL = () -> IAdhocGroupBy.GRAND_TOTAL;

	IAdhocGroupBy getGroupBy();
}