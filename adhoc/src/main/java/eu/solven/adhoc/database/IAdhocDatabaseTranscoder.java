package eu.solven.adhoc.database;

/**
 * Holds the logic mapping from the columns names in {@link eu.solven.adhoc.api.v1.IAdhocQuery} and columnNames in {@link IAdhocDatabaseWrapper}.
 * <p>
 * This enables re-using a {@link eu.solven.adhoc.dag.AdhocMeasureBag} for different {@link IAdhocDatabaseWrapper}.
 */
public interface IAdhocDatabaseTranscoder {
    /**
     *
     * @param queried a column name typically used by an {@link eu.solven.adhoc.api.v1.IAdhocQuery}.
     * @return the equivalent underlying column name, typically used by the database.
     */
    String underlying(String queried);
    /**
     *
     * @param underlying a column name typically used by the database.
     * @return the equivalent queried column name, typically used by an {@link eu.solven.adhoc.api.v1.IAdhocQuery}.
     */
    String queried(String underlying);
}
