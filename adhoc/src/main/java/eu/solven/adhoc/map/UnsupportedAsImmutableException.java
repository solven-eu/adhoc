package eu.solven.adhoc.map;

/**
 * Used for operations forbidden due to immutability.
 * 
 * @author Benoit Lacelle
 */
public class UnsupportedAsImmutableException extends UnsupportedOperationException {
	private static final long serialVersionUID = 5673313093256614656L;

	public UnsupportedAsImmutableException() {
		super("Immutable");
	}
}
