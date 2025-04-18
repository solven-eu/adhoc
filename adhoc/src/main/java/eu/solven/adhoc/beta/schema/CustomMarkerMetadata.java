package eu.solven.adhoc.beta.schema;

import java.util.Set;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Helps defining a customMarker single data-point. Typically used for UI read/write of customMarkers.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class CustomMarkerMetadata {
	@NonNull
	String name;
	
	// jsonPath to the data-point
	@NonNull
	String path;

	// String type;

	@NonNull
	Set<String> possibleValues;

	// May be null
	String defaultValue;

}
