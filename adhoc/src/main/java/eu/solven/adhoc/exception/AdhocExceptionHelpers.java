package eu.solven.adhoc.exception;

import java.util.concurrent.CompletionException;

import lombok.experimental.UtilityClass;

/**
 * Utilities related to {@link Exception} and {@link Throwable}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocExceptionHelpers {

	public static RuntimeException wrap(RuntimeException e, String eMsg) {
		if (e instanceof IllegalStateException illegalStateE) {
			// We want to keep bubbling an IllegalStateException
			return new IllegalStateException(eMsg, illegalStateE);
		} else if (e instanceof CompletionException completionE) {
			if (completionE.getCause() instanceof IllegalStateException) {
				// We want to keep bubbling an IllegalStateException
				return new IllegalStateException(eMsg, completionE);
			} else {
				return new IllegalArgumentException(eMsg, completionE);
			}
		} else {
			return new IllegalArgumentException(eMsg, e);
		}
	}

}
