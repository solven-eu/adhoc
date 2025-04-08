package eu.solven.adhoc.table.sql;

import eu.solven.adhoc.query.ICountMeasuresConstants;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Name;
import org.jooq.Parser;
import org.jooq.impl.DSL;
import org.jooq.impl.ParserException;

import java.util.function.Supplier;

/**
 * Utility methods to help coding with JooQ.
 */
@Slf4j
public class AdhocJooqHelper {

    /**
     *
     * @param columnName
     * @return true if the columnName is actually an expression, meaning it is not a real column.
     */
    public static boolean isExpression(String columnName) {
        if (ICountMeasuresConstants.ASTERISK.equals(columnName)) {
            // Typically on `COUNT(*)`
            return true;
        } else {
            return false;
        }
    }
    public static Name name(String name, Supplier<Parser> parserSupplier) {
        if (isExpression(name)) {
            // e.g. `name=*`
            return DSL.unquotedName(name);
        }

        Parser parser = parserSupplier.get();
        try {
            return parser
                    .parseName(name)
                    // quoted as we may receive `a@b@c` which has a special meaning in SQL
                    .quotedName();
        } catch (ParserException e) {
            if (log.isDebugEnabled()) {
                log.debug("Issue parsing `{}` as name. Quoting it ", name, e);
            } else {
                log.debug("Issue parsing `{}` as name. Quoting it ", name);
            }
            // BEWARE This is certainly buggy
            return DSL.quotedName(name.split("\\."));
        }
    }
}
