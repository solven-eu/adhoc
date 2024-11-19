package eu.solven.adhoc.database;

import eu.solven.adhoc.dag.AdhocMeasureBag;
import lombok.Builder;
import lombok.NonNull;

/**
 * An {@link AdhocCube} enables querying {@link eu.solven.adhoc.api.v1.IAdhocQuery}, over an {@link IAdhocDatabaseWrapper} with a predefined {@link IAdhocDatabaseTranscoder}.
 */
@Builder
public class AdhocCube {
    @NonNull
    AdhocMeasureBag measureBag;
    @NonNull
    IAdhocDatabaseWrapper databaseWrapper;
}
