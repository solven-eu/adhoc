package eu.solven.adhoc.query;

import eu.solven.adhoc.util.AdhocUnsafe;

/**
 * Helps switching Adhoc into Case-Sensitive or Case-Insensitive.
 * 
 * @author Benoit Lacelle
 */
public class AdhocCaseSensitivity {
	public static boolean isCaseSensitive() {
		return AdhocUnsafe.isCaseSensitive();
	}
}
