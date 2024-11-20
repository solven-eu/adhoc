package eu.solven.adhoc.api.v1.filters;

import java.util.Collection;

import javax.annotation.Nonnull;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.IValueMatcher;

/**
 * Filter along a specific column. Typically for `=` or `IN` matchers.
 *
 * @author Benoit Lacelle
 */
public interface IColumnFilter extends IAdhocFilter {

    /**
     * @return the name of the filtered column.
     */
    @Nonnull
    String getColumn();

    /**
     * The default is generally true, as it follows the fact that `Map.get(unknownKey)` returns null.
     *
     * @return true if a missing column would behave like containing NULL.
     */
    boolean isNullIfAbsent();

    /**
     * The filter could be null, a {@link Collection} for a `IN` clause, a {@link eu.solven.adhoc.api.v1.pojo.LikeMatcher}, else it is interpreted as an `=` clause.
     *
     * @return the filtered value.
     */
    IValueMatcher getValueMatcher();

}