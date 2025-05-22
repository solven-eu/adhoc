package eu.solven.adhoc.measure.combination;

/**
 * Some objects may have sanity checks. These may be verified eagerly or lazily.
 * 
 * @author Benoit Lacelle
 */
public interface IHasSanityChecks {
	/**
	 * Would throw if something is wrong. May log WARNs.
	 */
	void checkSanity();
}
