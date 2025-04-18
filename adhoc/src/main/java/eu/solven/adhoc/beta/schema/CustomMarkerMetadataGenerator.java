package eu.solven.adhoc.beta.schema;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.Value;

/**
 * Helps defining a customMarker single data-point. Typically used for UI read/write of customMarkers.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class CustomMarkerMetadataGenerator {
	// jsonPath to the data-point
	String path;

	// String type;

	Supplier<Set<String>> possibleValues;

	// Helps indicating the default value when none is provided
	Supplier<Optional<String>> defaultValue;

	public CustomMarkerMetadata snapshot(String name) {
		return CustomMarkerMetadata.builder()
				.name(name)
				.path(path)
				.possibleValues(possibleValues.get())
				.defaultValue(defaultValue.get().orElse(null))
				.build();
	}
}
