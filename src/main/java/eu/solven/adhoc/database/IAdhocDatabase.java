package eu.solven.adhoc.database;

import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.query.DatabaseQuery;

/**
 * Abstract a database storing data for {@link IAdhocQuery}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAdhocDatabase {

	Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery);

}
