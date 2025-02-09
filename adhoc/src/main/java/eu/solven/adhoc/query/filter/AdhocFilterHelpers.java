package eu.solven.adhoc.query.filter;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.pepper.core.PepperLogHelper;

import java.time.LocalDate;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Various helpers for {@link IAdhocFilter}
 */
public class AdhocFilterHelpers {
    protected AdhocFilterHelpers() {
        // hidden
    }

    /**
     *
     * @param filter some {@link IAdhocFilter}, potentially complex
     * @param column some specific column
     * @return
     */
    public Optional<IValueMatcher> getValueMatcher(IAdhocFilter filter, String column) {
        if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
            if (columnFilter.getColumn().equals(column)) {
                return Optional.of(columnFilter.getValueMatcher());
            } else {
                // column is not filtered
                return Optional.empty();
            }
        } else  if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
            Set<IValueMatcher> filters = andFilter.getOperands().stream().map(f -> getValueMatcher(f, column)).flatMap(Optional::stream).collect(Collectors.toSet());

            if(filters.isEmpty()) {
                return Optional.empty();
            } else if (filters.size() == 1) {
                return filters.stream().findFirst();
            } else {
                return Optional.of(AndMatcher.builder().operands(filters).build());
            }
        } else {
            throw new UnsupportedOperationException("filter=%s is not managed yet".formatted(PepperLogHelper.getObjectAndClass(filter)));
        }
    }
//
//
//    /**
//     *
//     * @param filter some {@link IAdhocFilter}, potentially complex
//     * @param column some specific column
//     * @param clazz the type of the expected value. Will throw if there is a filter with a not compatible type
//     * @return
//     * @param <T>
//     */
//    public <T> Optional<T> getEqualsOperand(IAdhocFilter filter, String column, Class<? extends T> clazz) {
//        Optional<IValueMatcher> optValueMatcher = getValueMatcher(filter, column);
//
//        return optvalueMatcher.map(valueMatcher -> {
//            if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
//                Object rawOperand = equalsMatcher.getOperand();
//
//                if (clazz.isInstance(rawOperand)) {
//                    T operandAsT = clazz.cast (rawOperand);
//                    return Optional.of(operandAsT);
//                } else {
//                    throw new IllegalArgumentException("operand=%s is not instance of %s".formatted(PepperLogHelper.getObjectAndClass(rawOperand), clazz));
//                }
//            } else {
//                // column is filtered but not with equals
//                return Optional.empty();
//            }
//        });
//    }
}
