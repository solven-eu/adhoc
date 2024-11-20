package eu.solven.adhoc.execute;

import java.util.Collection;
import java.util.Map;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.api.v1.filters.INotFilter;
import eu.solven.adhoc.api.v1.filters.IOrFilter;
import eu.solven.adhoc.api.v1.pojo.LikeMatcher;
import eu.solven.adhoc.database.IAdhocDatabaseTranscoder;
import eu.solven.adhoc.database.IdentityTranscoder;
import eu.solven.pepper.core.PepperLogHelper;

public class FilterHelpers {

    public static boolean match(IAdhocFilter filter, Map<String, ?> input) {
        return match(new IdentityTranscoder(), filter, input);
    }

    public static boolean match(IAdhocDatabaseTranscoder transcoder, IAdhocFilter filter, Map<String, ?> input) {
        if (filter.isAnd()) {
            IAndFilter andFilter = (IAndFilter) filter;
            return andFilter.getAnd().stream().allMatch(f -> match(f, input));
        } else if (filter.isOr()) {
            IOrFilter orFilter = (IOrFilter) filter;
            return orFilter.getOr().stream().anyMatch(f -> match(f, input));
        } else if (filter.isColumnMatcher()) {
            IColumnFilter columnFilter = (IColumnFilter) filter;

            String underlyingColumn = transcoder.underlying(columnFilter.getColumn());
            Object value = input.get(underlyingColumn);

            if (value == null && !input.containsKey(underlyingColumn) && !columnFilter.isNullIfAbsent()) {
                return false;
            }

            return columnFilter.getValueMatcher().match(value);
        } else if (filter.isNot()) {
            INotFilter notFilter = (INotFilter) filter;
            return !match(notFilter.getNegated(), input);
        } else {
            throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
        }
    }
}
