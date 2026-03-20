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
package eu.solven.adhoc.table.transcoder;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapTableStrictAliaser {

	@Test
	public void testUnderlying_knownAlias() {
		MapTableStrictAliaser aliaser = MapTableStrictAliaser.builder().aliasToOriginal("alias_price", "price").build();

		Assertions.assertThat(aliaser.underlying("alias_price")).isEqualTo("price");
	}

	@Test
	public void testUnderlying_unknownAlias_returnsNull() {
		MapTableStrictAliaser aliaser = MapTableStrictAliaser.builder().aliasToOriginal("alias_price", "price").build();

		Assertions.assertThat(aliaser.underlying("qty")).isNull();
	}

	@Test
	public void testGetAlias() {
		MapTableStrictAliaser aliaser =
				MapTableStrictAliaser.builder().aliasToOriginal("a", "col_a").aliasToOriginal("b", "col_b").build();

		Assertions.assertThat(aliaser.getAlias()).containsExactlyInAnyOrder("a", "b");
	}

	@Test
	public void testConflict_throws() {
		Assertions.assertThatThrownBy(() -> MapTableStrictAliaser.builder()
				.aliasToOriginal("alias", "col1")
				.aliasToOriginal("alias", "col2")
				.build()).isInstanceOf(IllegalArgumentException.class);
	}

}
