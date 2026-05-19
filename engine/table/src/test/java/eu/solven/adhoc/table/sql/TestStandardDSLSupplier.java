/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.table.sql;

import org.assertj.core.api.Assertions;
import org.jooq.Name;
import org.jooq.SQLDialect;
import org.jooq.conf.ParseNameCase;
import org.junit.jupiter.api.Test;

/**
 * Pins the caseSensitivity wiring of {@link StandardDSLSupplier}. The supplier injects
 * {@link AdhocCaseInsensitivityUnsafe#jooqSettings(boolean)} into the JOOQ configuration; when caseSensitive, the
 * settings set {@code ParseNameCase.AS_IS} so the parser preserves the user's chosen identifier casing. When
 * caseInsensitive the setting is omitted and JOOQ falls back to its dialect-default name-case behaviour (which, for the
 * DuckDB dialect the tests run under, happens to also preserve case — so the observable
 * {@link AdhocJooqHelper#name(String, java.util.function.Supplier)} output is the same; the difference is encoded in
 * the configuration itself, which the dedicated test verifies).
 *
 * <p>
 * Each name-render test runs through {@link AdhocJooqHelper#name(String, java.util.function.Supplier)} — the entry
 * point Adhoc uses everywhere it resolves a user-supplied column reference into a JOOQ {@link Name} — so the assertion
 * exercises the same code path production does.
 */
public class TestStandardDSLSupplier {
	static {
		AdhocJooqHelper.disableBanners();
	}

	@Test
	public void testCaseSensitiveBuilder_setsParseNameCaseAsIs() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(false).dialect(SQLDialect.DUCKDB).build();

		// Direct check on the underlying settings: caseSensitive must inject AS_IS so the JOOQ
		// parser never normalises identifier case. The observable end-to-end behaviour is
		// covered by the name-rendering tests below; this assertion pins the wiring itself
		// (the source of truth) so a refactor that drops the settings injection fails here
		// even if the surrounding dialect's defaults happen to mask it.
		Assertions.assertThat(supplier.getDSLContext().settings().getParseNameCase()).isEqualTo(ParseNameCase.AS_IS);
	}

	@Test
	public void testCaseInsensitiveBuilder_keepsParseNameCaseDefault() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(true).dialect(SQLDialect.DUCKDB).build();

		// When caseInsensitive, the supplier does NOT pin AS_IS. The Settings field stays at
		// JOOQ's `ParseNameCase.DEFAULT` enum value (the bean's initialised default), meaning
		// the dialect's own rule takes over. The contract is "do not pin AS_IS"; whether DEFAULT
		// upper-cases, lower-cases, or preserves identifiers is then up to the dialect.
		Assertions.assertThat(supplier.getDSLContext().settings().getParseNameCase()).isEqualTo(ParseNameCase.DEFAULT);
	}

	@Test
	public void testCaseSensitiveBuilder_preservesIdentifierCaseViaParser() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(false).dialect(SQLDialect.DUCKDB).build();

		Name name = AdhocJooqHelper.name("MixedCase", supplier.getDSLContext()::parser);

		// ParseNameCase.AS_IS keeps the casing the user typed — the rendered name carries
		// the exact "MixedCase" between the JOOQ quotes.
		Assertions.assertThat(name.toString()).isEqualTo("\"MixedCase\"");
	}

	@Test
	public void testCaseSensitiveBuilder_preservesCaseOnQualifiedName() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(false).dialect(SQLDialect.DUCKDB).build();

		// A two-part qualifier like "Schema.Table" parses into a two-segment Name. Both
		// segments retain their original casing under AS_IS.
		Name name = AdhocJooqHelper.name("Schema.Table", supplier.getDSLContext()::parser);

		Assertions.assertThat(name.toString()).isEqualTo("\"Schema\".\"Table\"");
	}

	@Test
	public void testDefaultBuilder_followsAdhocCaseInsensitivityUnsafeDefault() {
		// The no-arg `StandardDSLSupplier.builder()` reads the live
		// `AdhocCaseInsensitivityUnsafe.isCaseInsensitive()` flag. Adhoc's default is
		// case-sensitive, so the no-arg builder must inject AS_IS just like `builder(false)`
		// does. The test asserts both the flag value AND the resulting settings end-to-end.
		Assertions.assertThat(AdhocCaseInsensitivityUnsafe.isCaseInsensitive()).isFalse();

		StandardDSLSupplier supplier = StandardDSLSupplier.builder().dialect(SQLDialect.DUCKDB).build();

		Assertions.assertThat(supplier.getDSLContext().settings().getParseNameCase()).isEqualTo(ParseNameCase.AS_IS);
		Name name = AdhocJooqHelper.name("MixedCase", supplier.getDSLContext()::parser);
		Assertions.assertThat(name.toString()).isEqualTo("\"MixedCase\"");
	}

	@Test
	public void testNameWithSpaces_preservedCaseSensitive() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(false).dialect(SQLDialect.DUCKDB).build();

		// Whitespace inside an identifier forces JOOQ's parser to keep the whole thing as a
		// single quoted name; the casing is preserved under AS_IS.
		Name name = AdhocJooqHelper.name("Player Name", supplier.getDSLContext()::parser);

		Assertions.assertThat(name.toString()).isEqualTo("\"Player Name\"");
	}

	@Test
	public void testNameWithSpaces_preservedCaseInsensitive_DuckDB() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(true).dialect(SQLDialect.DUCKDB).build();

		Assertions.assertThat(AdhocJooqHelper.name("PlayerName", supplier.getDSLContext()::parser).toString())
				.isEqualTo("\"PlayerName\"");

		Assertions.assertThat(AdhocJooqHelper.name("Player Name", supplier.getDSLContext()::parser).toString())
				.isEqualTo("\"Player Name\"");
	}

	@Test
	public void testNameWithSpaces_preservedCaseInsensitive_PostgreSQL() {
		StandardDSLSupplier supplier = StandardDSLSupplier.builder(true).dialect(SQLDialect.POSTGRES).build();

		Assertions.assertThat(AdhocJooqHelper.name("PlayerName", supplier.getDSLContext()::parser).toString())
				.isEqualTo("\"playername\"");

		Assertions.assertThat(AdhocJooqHelper.name("Player Name", supplier.getDSLContext()::parser).toString())
				.isEqualTo("\"Player Name\"");
	}

	@Test
	public void testAsterisk_neverParsedRegardlessOfCaseSettings() {
		// `*` is the COUNT(*) marker — `AdhocJooqHelper.name` short-circuits at the
		// `isExpression(...)` check and emits an UNQUOTED `*` Name without invoking the parser.
		// The case-sensitivity setting therefore can't affect this path; the test pins that
		// short-circuit so a future refactor doesn't accidentally drop the parser-bypass.
		StandardDSLSupplier caseSensitive = StandardDSLSupplier.builder(false).dialect(SQLDialect.DUCKDB).build();
		StandardDSLSupplier caseInsensitive = StandardDSLSupplier.builder(true).dialect(SQLDialect.DUCKDB).build();

		Assertions.assertThat(AdhocJooqHelper.name("*", caseSensitive.getDSLContext()::parser).toString())
				.isEqualTo("*");
		Assertions.assertThat(AdhocJooqHelper.name("*", caseInsensitive.getDSLContext()::parser).toString())
				.isEqualTo("*");
	}
}
