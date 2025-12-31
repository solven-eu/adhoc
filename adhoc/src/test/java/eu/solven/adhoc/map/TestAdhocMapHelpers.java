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
package eu.solven.adhoc.map;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;

public class TestAdhocMapHelpers {
	@Test
	public void testFromMapIdentity() {
		ISliceFactory sliceFactory = AdhocFactoriesUnsafe.factories.getSliceFactoryFactory().makeFactory();

		IAdhocMap original = sliceFactory.newMapBuilder().put("c", "v").build();
		ITabularRecord originalRecord =
				TabularRecordOverMaps.builder().groupBy(original.asSlice()).aggregate("a", 123L).build();

		ITabularRecord transcoded = originalRecord.transcode(new IColumnValueTranscoder() {

			@Override
			public Object transcodeValue(String column, Object value) {
				return value;
			}

			@Override
			public Set<String> mayTranscode(Set<String> columns) {
				return Set.of();
			}
		});

		Assertions.assertThat(transcoded).isNotSameAs(originalRecord).isEqualTo(originalRecord);
		Assertions.assertThat((Map) transcoded.getGroupBys().asAdhocMap()).isSameAs(original);
	}
}
