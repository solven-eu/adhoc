package eu.solven.adhoc.database;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * A transcoder useful when it is known that all columns has a redundant prefix (e.g. from SQL schema).
 * 
 * @author Benoit Lacelle
 *
 */
@Builder
public class PrefixTranscoder implements  IAdhocDatabaseTranscoder{
	// If empty, it is like the IdentityTranscoder
	@NonNull
	@Default
	String prefix = "";

	@Override
	public String underlying(String queried) {
		return prefix + queried;
	}

	@Override
	public String queried(String underlying) {
		if (underlying.startsWith(prefix)) {
			return underlying.substring(prefix.length());
		} else {
			throw new IllegalArgumentException("We received a column not prefixed by %s: %s".formatted(prefix, underlying));
		}
	}
}
