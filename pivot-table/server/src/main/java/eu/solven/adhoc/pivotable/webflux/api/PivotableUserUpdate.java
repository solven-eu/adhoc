package eu.solven.adhoc.pivotable.webflux.api;

import java.util.Optional;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class PivotableUserUpdate {
	// This is nullable as it may not be updated
	@Default
	Optional<String> countryCode = Optional.empty();
}
