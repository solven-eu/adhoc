/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

public class TestRecursiveAliaser {
	@Test
	public void testRecursive() {
		ITableAliaser aliaser = RecursiveAliaser
				.wrap(MapTableAliaser.builder().aliasToOriginal("k", "_k").aliasToOriginal("_k", "__k").build());

		Assertions.assertThat(aliaser.underlying("a")).isEqualTo(null);
		Assertions.assertThat(aliaser.underlying("k")).isEqualTo("__k");
		Assertions.assertThat(aliaser.underlying("_k")).isEqualTo("__k");
		Assertions.assertThat(aliaser.underlying("__k")).isEqualTo(null);
	}

	@Test
	public void testRecursive_cycle() {
		ITableAliaser aliaser = RecursiveAliaser.wrap(MapTableAliaser.builder()
				.aliasToOriginal("k", "_k")
				.aliasToOriginal("_k", "__k")
				.aliasToOriginal("__k", "k")
				.build());

		Assertions.assertThat(aliaser.underlying("a")).isEqualTo(null);
		Assertions.assertThatThrownBy(() -> aliaser.underlying("k"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Transcoding cycle from `k` and through: [k, _k, __k, k]");
		Assertions.assertThatThrownBy(() -> aliaser.underlying("_k"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Transcoding cycle from `_k` and through: [_k, __k, k, _k]");
		Assertions.assertThatThrownBy(() -> aliaser.underlying("__k"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Transcoding cycle from `__k` and through: [__k, k, _k, __k]");
	}

	@Test
	public void testRecursive_veryDeep() {
		var aliaserBuilder = MapTableAliaser.builder();

		int depth = 16 * 1024;
		for (int i = 0; i <= depth; i++) {
			aliaserBuilder.aliasToOriginal("k_" + i, "k_" + (i + 1));
		}
		ITableAliaser transcoder = RecursiveAliaser.wrap(aliaserBuilder.build());

		Assertions.assertThat(transcoder.underlying("k_" + (depth - 1))).isEqualTo("k_" + (depth + 1));
		Assertions.assertThatThrownBy(() -> transcoder.underlying("k_" + 0))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Transcoding too-deep from `k_0` and through: [k_0, k_1, k_2, k_3, k_4, k")
				.hasMessageContaining(", k_1019, k_1020, k_1021, k_1022, k_1023]");
	}
}
