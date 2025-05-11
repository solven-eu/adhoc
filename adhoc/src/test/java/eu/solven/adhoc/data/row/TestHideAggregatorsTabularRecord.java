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
package eu.solven.adhoc.data.row;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.table.transcoder.IdentityReversibleTranscoder;

public class TestHideAggregatorsTabularRecord {
	@Test
	public void testNominalCase() {
		TabularRecordOverMaps underlying = TabularRecordOverMaps.builder()
				.slice(Map.of("c", "c1"))
				.aggregate("k1", 123)
				.aggregate("k2", 234)
				.build();
		HideAggregatorsTabularRecord record = HideAggregatorsTabularRecord.builder()
				.decorated(underlying)
				.keptAggregates(ImmutableSet.of("k1"))
				.build();

		ITabularRecord transcoded = record.transcode(new IdentityReversibleTranscoder());

		Assertions.assertThat((Map) transcoded.asMap()).containsEntry("c", "c1").containsEntry("k1", 123L).hasSize(2);

		// The trailing `, ` is a minor bug
		Assertions.assertThat(transcoded.toString()).isEqualTo("slice:{c=c1} aggregates:{k1=123}");
	}

	@Test
	public void testKeepNotPresent() {
		TabularRecordOverMaps underlying = TabularRecordOverMaps.builder()
				.slice(Map.of("c", "c1"))
				.aggregate("k1", 123)
				.aggregate("k2", 234)
				.build();
		HideAggregatorsTabularRecord record = HideAggregatorsTabularRecord.builder()
				.decorated(underlying)
				.keptAggregates(ImmutableSet.of("k1", "k3"))
				.build();

		ITabularRecord transcoded = record.transcode(new IdentityReversibleTranscoder());

		Assertions.assertThat((Map) transcoded.asMap()).containsEntry("c", "c1").containsEntry("k1", 123L).hasSize(2);

		// The trailing `, ` is a minor bug
		Assertions.assertThat(transcoded.toString()).isEqualTo("slice:{c=c1} aggregates:{k1=123}");
	}
}
