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
package eu.solven.adhoc.example.worldcup;

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicLongMap;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Collects the events {@link String} (e.g. `G13'`) into an object.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class PlayersEvents {
	@JsonIgnore
	@Singular
	@NonNull
	ImmutableMap<String, AtomicLongMap<Integer>> typeToMinuteToCounts;

	public static PlayersEvents merge(PlayersEvents left, PlayersEvents right) {
		Map<String, AtomicLongMap<Integer>> typeToMinuteToCount = new TreeMap<>();

		left.getTypeToMinuteToCounts().forEach((key, values) -> {
			AtomicLongMap<Integer> atomicMap = typeToMinuteToCount.computeIfAbsent(key, k -> AtomicLongMap.create());
			values.asMap().forEach(atomicMap::addAndGet);
		});

		right.getTypeToMinuteToCounts().forEach((key, values) -> {
			AtomicLongMap<Integer> atomicMap = typeToMinuteToCount.computeIfAbsent(key, k -> AtomicLongMap.create());
			values.asMap().forEach(atomicMap::addAndGet);
		});

		return PlayersEvents.builder().typeToMinuteToCounts(typeToMinuteToCount).build();
	}

	// For Jackson representation, so such aggregates can be printed in Pivotable
	@JsonValue
	public Map<String, Map<Integer, Long>> getTypeToMinuteToCount() {
		return getTypeToMinuteToCounts().entrySet()
				.stream()
				.collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().asMap()));
	}
}
