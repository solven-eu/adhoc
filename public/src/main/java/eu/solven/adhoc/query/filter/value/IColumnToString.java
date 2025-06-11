package eu.solven.adhoc.query.filter.value;

/**
 * Some {@link IValueMatcher} may have a custom `.toString` for nicer human representation.
 */
@FunctionalInterface
public interface IColumnToString {
    /**
     *
     * @param column
     * @param negated true if this {@link IValueMatcher} is wrapper in a {@link NotMatcher}
     * @return
     */
    String toString(String column, boolean negated);
}
