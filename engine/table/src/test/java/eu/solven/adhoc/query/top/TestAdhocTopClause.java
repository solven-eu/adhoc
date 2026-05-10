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
package eu.solven.adhoc.query.top;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.model.column.ReferencedColumn;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestAdhocTopClause {

	@Test
	public void testEqualsHashCode() {
		EqualsVerifier.forClass(AdhocTopClause.class).verify();
	}

	@Test
	public void testNoLimit_isEmpty() {
		AdhocTopClause clause = AdhocTopClause.NO_LIMIT;

		Assertions.assertThat(clause.isPresent()).isFalse();
		Assertions.assertThat(clause.getColumns()).isEmpty();
		// Default limit is the negative NO_TOP sentinel — toString collapses to "noLimit".
		Assertions.assertThat(clause).hasToString("noLimit");
	}

	@Test
	public void testWithColumnsAndLimit_isPresent() {
		AdhocTopClause clause = AdhocTopClause.builder().column(ReferencedColumn.ref("a")).limit(10).build();

		Assertions.assertThat(clause.isPresent()).isTrue();
		Assertions.assertThat(clause.getColumns()).hasSize(1);
		Assertions.assertThat(clause.getLimit()).isEqualTo(10);
		// Default desc is true.
		Assertions.assertThat(clause.isDesc()).isTrue();
		Assertions.assertThat(clause).hasToString("limit=10 desc=true columns=[ReferencedColumn(name=a)]");
	}

	@Test
	public void testDescFlagFalse_appearsInToString() {
		AdhocTopClause clause = AdhocTopClause.builder().column(ReferencedColumn.ref("a")).limit(5).desc(false).build();

		Assertions.assertThat(clause.isDesc()).isFalse();
		Assertions.assertThat(clause).hasToString("limit=5 desc=false columns=[ReferencedColumn(name=a)]");
	}
}
