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
package eu.solven.adhoc.fsst;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.column.StandardFreezingStrategy;
import eu.solven.adhoc.compression.page.IReadableColumn;

public class TestFsstFreezingStrategy {
	StandardFreezingStrategy s =
			StandardFreezingStrategy.builder().freezersWithContext(List.of(new FsstFreezingWithContext())).build();

	@Test
	public void testStrings() {

		IAppendableColumn column = ObjectArrayColumn.builder().freezer(s).asArray(new ArrayList<>()).build();

		column.append("a");
		column.append("b");

		IReadableColumn frozen = column.freeze();

		Assertions.assertThat(frozen).isInstanceOf(FsstReadableColumn.class);
		Assertions.assertThat(frozen.readValue(0)).isEqualTo("a");
		Assertions.assertThat(frozen.readValue(1)).isEqualTo("b");
	}

	@Test
	public void testStrings_withNull() {
		IAppendableColumn column = ObjectArrayColumn.builder().freezer(s).asArray(new ArrayList<>()).build();

		column.append("a");
		column.append(null);
		column.append("b");

		IReadableColumn frozen = column.freeze();

		Assertions.assertThat(frozen).isInstanceOf(FsstReadableColumn.class);
		Assertions.assertThat(frozen.readValue(0)).isEqualTo("a");
		Assertions.assertThat(frozen.readValue(1)).isEqualTo(null);
		Assertions.assertThat(frozen.readValue(2)).isEqualTo("b");
	}
}
