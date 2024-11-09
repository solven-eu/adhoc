package eu.solven.adhoc.transformers;

import java.util.List;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class Combinator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	List<String> underlyingMeasurators;

	@NonNull
	String transformationKey;

}
