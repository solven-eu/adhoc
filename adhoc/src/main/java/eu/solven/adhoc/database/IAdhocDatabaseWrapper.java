package eu.solven.adhoc.database;

import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.query.DatabaseQuery;

/**
 * Wraps a database (actually storing data for {@link IAdhocQuery}) to be queried by {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAdhocDatabaseWrapper {
	/**
	 *
	 * @return the {@link IAdhocDatabaseTranscoder} used by this wrapper.
	 */
	IAdhocDatabaseTranscoder getTranscoder();

	Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery);

}
