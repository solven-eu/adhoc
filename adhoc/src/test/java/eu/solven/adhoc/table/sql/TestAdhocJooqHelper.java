package eu.solven.adhoc.table.sql;

import org.assertj.core.api.Assertions;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

public class TestAdhocJooqHelper {
    static {
        // https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
        System.setProperty("org.jooq.no-logo", "true");
        // https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
        System.setProperty("org.jooq.no-tips", "true");
    }

    @Test
    public void testIsExpression() {
        Assertions.assertThat(AdhocJooqHelper.isExpression("foo")).isFalse();
        Assertions.assertThat(AdhocJooqHelper.isExpression("*")).isTrue();
    }
    @Test
    public void testName() {
        Assertions.assertThat(AdhocJooqHelper.name("foo", DSL.using(SQLDialect.DUCKDB)::parser).toString()).isEqualTo("\"foo\"");
        Assertions.assertThat(AdhocJooqHelper.name("foo.bar", DSL.using(SQLDialect.DUCKDB)::parser).toString()).isEqualTo("\"foo\".\"bar\"");
        Assertions.assertThat(AdhocJooqHelper.name("foo bar", DSL.using(SQLDialect.DUCKDB)::parser).toString()).isEqualTo("\"foo bar\"");

        // This case is not very legitimate from standard SQL perspective
        // Still, we parse it as if whitespace was not a special character
        Assertions.assertThat(AdhocJooqHelper.name("foo.bar zoo", DSL.using(SQLDialect.DUCKDB)::parser).toString()).isEqualTo("\"foo\".\"bar zoo\"");
    }
}
