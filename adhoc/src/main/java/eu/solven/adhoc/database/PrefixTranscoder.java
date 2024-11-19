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
public class PrefixTranscoder {
	// If empty, it is like the IdentityTranscoder
	@NonNull
	@Default
	String prefix = "";
}
