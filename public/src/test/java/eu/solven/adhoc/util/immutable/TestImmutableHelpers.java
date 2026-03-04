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
package eu.solven.adhoc.util.immutable;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Builder;

public class TestImmutableHelpers {
	@Builder
	static class CustomImmutableList extends AbstractList<String> implements IImmutable {
		final List<String> decorated;

		@Override
		public int size() {
			return decorated.size();
		}

		@Override
		public String get(int index) {
			return decorated.get(index);
		}

	}

	@Test
	public void testCopyList() {
		List<String> list = List.of("a");
		ImmutableList<String> immutableList = ImmutableList.copyOf(list);
		List<String> customImmutable = CustomImmutableList.builder().decorated(list).build();

		Assertions.assertThat(ImmutableHelpers.copyOf(immutableList)).isSameAs(immutableList);
		Assertions.assertThat(ImmutableHelpers.copyOf(list)).isNotSameAs(immutableList).isEqualTo(immutableList);
		Assertions.assertThat(ImmutableHelpers.copyOf(customImmutable)).isSameAs(customImmutable);
	}

	@Builder
	static class CustomImmutableMap extends AbstractMap<String, String> implements IImmutable {
		final Map<String, String> decorated;

		@Override
		public Set<Entry<String, String>> entrySet() {
			return decorated.entrySet();
		}

	}

	@Test
	public void testCopyMap() {
		Map<String, String> map = Map.of("a", "a1");
		ImmutableMap<String, String> immutableMap = ImmutableMap.copyOf(map);
		Map<String, String> customImmutableMap = CustomImmutableMap.builder().decorated(map).build();

		Assertions.assertThat(ImmutableHelpers.copyOf(immutableMap)).isSameAs(immutableMap);
		Assertions.assertThat(ImmutableHelpers.copyOf(map)).isNotSameAs(immutableMap).isEqualTo(immutableMap);
		Assertions.assertThat(ImmutableHelpers.copyOf(customImmutableMap)).isSameAs(customImmutableMap);
	}
}
