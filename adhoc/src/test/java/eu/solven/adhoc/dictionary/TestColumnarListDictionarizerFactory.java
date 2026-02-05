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
package eu.solven.adhoc.dictionary;

import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.ColumnarListDictionarizerFactory;
import eu.solven.adhoc.compression.IListDictionarizer;
import eu.solven.adhoc.compression.IListDictionarizerFactory;
import eu.solven.adhoc.map.keyset.NavigableSetLikeList;

public class TestColumnarListDictionarizerFactory {
	IListDictionarizerFactory factory = new ColumnarListDictionarizerFactory(5);

	@Test
	public void testMakeMap() {
		IListDictionarizer dictionarizer = factory.makeDictionarizer(NavigableSetLikeList.fromSet(Set.of("a", "b")));

		IntFunction<Object> dic00 = dictionarizer.dictionarize(List.of("a0", "b0"));
		Assertions.assertThat(dic00.apply(0)).isEqualTo("a0");
		Assertions.assertThat(dic00.apply(1)).isEqualTo("b0");

		IntFunction<Object> dic11 = dictionarizer.dictionarize(List.of("a1", "b1"));
		Assertions.assertThat(dic11.apply(0)).isEqualTo("a1");
		Assertions.assertThat(dic11.apply(1)).isEqualTo("b1");

		IntFunction<Object> dic10 = dictionarizer.dictionarize(List.of("a1", "b0"));
		Assertions.assertThat(dic10.apply(0)).isEqualTo("a1");
		Assertions.assertThat(dic10.apply(1)).isEqualTo("b0");
	}
}
