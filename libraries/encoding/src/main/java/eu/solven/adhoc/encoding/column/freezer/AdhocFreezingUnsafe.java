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
package eu.solven.adhoc.encoding.column.freezer;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.encoding.dictionary.DistinctFreezer;
import eu.solven.adhoc.encoding.string.FsstFreezingWithContext;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Some various unsafe constants, related to {@link IAdhocColumn} freezing.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
@SuppressWarnings("PMD.FieldDeclarationsShouldBeAtStartOfClass")
public class AdhocFreezingUnsafe {

	public void resetToDefaults() {
		freezers = DEFAULT_FREEZERS;
		checkPostCompression = DEFAULT_CHECK_POST_COMPRESSION;
	}

	// Freezers applied AFTER DistinctFreezer in the main chain — also passed to DistinctFreezer so that the
	// (small) dictionary of distinct values is itself processed by these strategies. This way, e.g. a low-cardinality
	// Utf8ByteSlice column that gets dictionarised still has its dictionary entries normalised to String by
	// Utf8ToStringFreezer (or compressed via FSST), instead of leaking raw Utf8ByteSlice instances downstream.
	private static final List<IFreezingWithContext> POST_DISTINCT_FREEZERS =
			ImmutableList.<IFreezingWithContext>builder()
					.add(new LongFreezer())
					.add(new IntegerFreezer())
					.add(new FsstFreezingWithContext())
					// Fallback: normalise any remaining AdhocUtf8 values to String when FSST did not fire
					// (e.g. mixed-type columns where FSST only handles pure-text columns)
					.add(new Utf8ToStringFreezer())
					.build();

	private static final List<IFreezingWithContext> DEFAULT_FREEZERS = ImmutableList.<IFreezingWithContext>builder()
			.add(new DistinctFreezer(POST_DISTINCT_FREEZERS))
			.addAll(POST_DISTINCT_FREEZERS)
			.build();

	@Getter
	@Setter
	private static List<IFreezingWithContext> freezers = DEFAULT_FREEZERS;

	private static final boolean DEFAULT_CHECK_POST_COMPRESSION = false;

	/**
	 * If true, each compression algorithm will check right after compression the decompression returns correct results.
	 */
	@Getter
	@Setter
	private static boolean checkPostCompression = DEFAULT_CHECK_POST_COMPRESSION;

}
