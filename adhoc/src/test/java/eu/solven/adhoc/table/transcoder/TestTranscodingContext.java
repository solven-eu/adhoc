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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestTranscodingContext {
	@Test
	public void testAddIdentity() {
		ITableAliaser aliaser = Mockito.mock(ITableAliaser.class, Mockito.CALLS_REAL_METHODS);

		AliasingContext context = AliasingContext.builder().aliaser(aliaser).build();

		Assertions.assertThat(context.underlying("c")).isEqualTo("c");
		Assertions.assertThat(context.queried("c")).containsExactly("c");

		Assertions.assertThat(context.underlyings()).containsExactlyInAnyOrder("c");
		Assertions.assertThat(context.estimateQueriedSize(Set.of("c"))).isEqualTo(1);

		// Implementation specific
		Assertions.assertThat(context.identity).contains("c");
		Assertions.assertThat(context.originalToAlias.asMap()).isEmpty();
	}

	@Test
	public void testAddMapped() {
		ITableAliaser aliaser = Mockito.mock(ITableAliaser.class, Mockito.CALLS_REAL_METHODS);

		Mockito.when(aliaser.underlying("cubeC")).thenReturn("tableC");

		AliasingContext context = AliasingContext.builder().aliaser(aliaser).build();

		Assertions.assertThat(context.underlying("cubeC")).isEqualTo("tableC");
		Assertions.assertThat(context.queried("tableC")).containsExactly("cubeC");
		Assertions.assertThat(context.estimateQueriedSize(Set.of("tableC"))).isEqualTo(1);

		Assertions.assertThat(context.underlyings()).containsExactlyInAnyOrder("tableC");

		// Implementation specific
		Assertions.assertThat(context.identity).isEmpty();
		Assertions.assertThat(context.originalToAlias.asMap()).containsEntry("tableC", Set.of("cubeC"));
	}

	@Test
	public void testAddIdentityAndMapped() {
		ITableAliaser aliaser = Mockito.mock(ITableAliaser.class, Mockito.CALLS_REAL_METHODS);

		Mockito.when(aliaser.underlying("cubeC")).thenReturn("tableC");
		Mockito.when(aliaser.underlying("tableC")).thenReturn("tableC");

		AliasingContext context = AliasingContext.builder().aliaser(aliaser).build();

		Assertions.assertThat(context.underlying("cubeC")).isEqualTo("tableC");
		Assertions.assertThat(context.underlying("tableC")).isEqualTo("tableC");
		Assertions.assertThat(context.queried("tableC")).contains("cubeC", "tableC").hasSize(2);
		Assertions.assertThat(context.estimateQueriedSize(Set.of("tableC"))).isEqualTo(2);

		Assertions.assertThat(context.underlyings()).containsExactlyInAnyOrder("tableC");

		// Implementation specific
		Assertions.assertThat(context.identity).contains("tableC");
		Assertions.assertThat(context.originalToAlias.asMap()).containsEntry("tableC", Set.of("cubeC"));
	}
}
