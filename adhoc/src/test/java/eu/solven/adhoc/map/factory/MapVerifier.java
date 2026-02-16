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
package eu.solven.adhoc.map.factory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;

import lombok.Builder;
import lombok.NonNull;

/**
 * Helps verifying a {@link Map} instance follows standard {@link Map} contract.
 * 
 * @author Benoit Lacelle
 */
public class MapVerifier {
	@Builder
	public static class MapVerifierInstance {
		@NonNull
		Map<?, ?> toVerify;

		// TODO handle recursive custom Map as key or value
		public void verify() {
			Map<?, ?> rawMap = new LinkedHashMap<>(toVerify);
			Assertions.assertThat((Map) toVerify)
					.hasToString(rawMap.toString())
					.isEqualTo(rawMap)
					.hasSameHashCodeAs(rawMap);

			Set<?> rawKeySet = rawMap.keySet();
			Assertions.assertThat(toVerify.keySet())
					.hasToString(rawKeySet.toString())
					.isEqualTo(rawKeySet)
					.hasSameHashCodeAs(rawKeySet);

			Set<?> rawEntrySet = rawMap.entrySet();
			Assertions.assertThat(toVerify.entrySet())
					.hasToString(rawEntrySet.toString())
					.isEqualTo(rawEntrySet)
					.hasSameHashCodeAs(rawEntrySet);
		}
	}

	public static MapVerifierInstance forInstance(Map<?, ?> map) {
		return MapVerifierInstance.builder().toVerify(map).build();
	}
}
