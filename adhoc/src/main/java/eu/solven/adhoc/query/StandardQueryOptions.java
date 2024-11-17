package eu.solven.adhoc.query;

/**
 * Various standard/not-exotic options for querying.
 * 
 * @author Benoit Lacelle
 *
 */
public enum StandardQueryOptions implements IQueryOption {
	/**
	 * All underlying measures are kept in the output result. This is relevant as it does not induces additional
	 * computations, but it induces additional RAM consumptions (as these implicitly requested measures can not be
	 * discarded).
	 */
	RETURN_UNDERLYING_MEASURES;
}
