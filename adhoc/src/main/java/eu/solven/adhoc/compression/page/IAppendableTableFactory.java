package eu.solven.adhoc.compression.page;

import eu.solven.adhoc.query.cube.IHasQueryOptions;

/**
 * Holds the logic to make {@link IAppendableTable}.
 *
 * @author Benoit Lacelle
 */
public interface IAppendableTableFactory {
    IAppendableTable makeTable(IHasQueryOptions options);
}
