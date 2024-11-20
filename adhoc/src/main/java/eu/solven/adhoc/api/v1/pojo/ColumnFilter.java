package eu.solven.adhoc.api.v1.pojo;

import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.Collection;
import java.util.stream.Collectors;

@Value
@Builder
@Jacksonized
public class ColumnFilter implements IColumnFilter {

    @NonNull
    final String column;

    @NonNull
    final IValueMatcher valueMatcher;

    // if the tested input is null, we return this flag
    @Builder.Default
    final boolean nullIfAbsent = true;

    @Override
    public boolean isNot() {
        return false;
    }

    @Override
    public boolean isColumnMatcher() {
        return true;
    }

    @Override
    public IValueMatcher getValueMatcher() {
        return valueMatcher;
    }

    public static class ColumnFilterBuilder {
        public ColumnFilterBuilder matchNull() {
            this.valueMatcher = NullMatcher.matchNull();
            return this;
        }

        public ColumnFilterBuilder matchIn(Collection<?> collection) {
            // One important edge-case is getting away from java.util.Set which generates NPE on .contains(null)
            this.valueMatcher = InMatcher.builder().operands(collection.stream().map(o -> {
                if (o == null) {
                    return NullMatcher.matchNull();
                } else {
                    return o;
                }
            }).collect(Collectors.toSet())).build();
            ;
            return this;
        }

        public ColumnFilterBuilder matchEquals(Object o) {
            this.valueMatcher = EqualsMatcher.builder().operand(o).build();
            return this;
        }

        public ColumnFilterBuilder matching(Object matching) {
            if (matching == null) {
                return matchNull();
            } else if (matching instanceof IValueMatcher vm) {
                this.valueMatcher = vm;
                return this;
            } else if (matching instanceof Collection<?> c) {
                return matchIn(c);
            } else if (matching instanceof IColumnFilter) {
                throw new IllegalArgumentException("Can not use a columnFilter as valueFilter: %s".formatted(PepperLogHelper.getObjectAndClass(matching)));
            } else {
                return matchEquals(matching);
            }
        }
    }

    /**
     * If matching is null, prefer
     *
     * @param column
     * @param matching
     * @return true if the column holds the same value as matching (following {@link Object#equals(Object)}) contract.
     */
    public static ColumnFilter isEqualTo(String column, Object matching) {
        return ColumnFilter.builder().column(column).matchEquals(matching).build();
    }

    /**
     * @param column
     * @param matching
     * @return true if the value is different, including if the value is null.
     */
    // https://stackoverflow.com/questions/36508815/not-equal-and-null-in-postgres
    public static ColumnFilter isDistinctFrom(String column, Object matching) {
        NotValueFilter not = NotValueFilter.builder().negated(EqualsMatcher.builder().operand(matching).build()).build();
        return ColumnFilter.builder().column(column).matching(not).build();
    }
}