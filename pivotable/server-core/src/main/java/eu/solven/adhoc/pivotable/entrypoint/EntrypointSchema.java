package eu.solven.adhoc.pivotable.entrypoint;

import eu.solven.adhoc.beta.schema.SchemaMetadata;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class EntrypointSchema {
	AdhocEntrypointMetadata entrypoint;

	SchemaMetadata schema;
}
