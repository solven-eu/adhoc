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
package eu.solven.adhoc.compression.column.freezer;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.compression.dictionary.DistinctFreezer;
import eu.solven.adhoc.fsst.FsstFreezingWithContext;
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
@SuppressWarnings("PMD.MutableStaticState")
public class AdhocFreezingUnsafe {

	public void resetToDefaults() {
		freezers = DEFAULT_FREEZERS;
		checkPostCompression = DEFAULT_CHECK_POST_COMPRESSION;
	}

	private static final List<IFreezingWithContext> DEFAULT_FREEZERS = ImmutableList.<IFreezingWithContext>builder()
			.add(new DistinctFreezer())
			.add(new LongFreezer())
			.add(new FsstFreezingWithContext())
			.build();

	@Getter
	@Setter
	protected static List<IFreezingWithContext> freezers = DEFAULT_FREEZERS;

	private static final boolean DEFAULT_CHECK_POST_COMPRESSION = false;

	/**
	 * If true, each compression algorithm will check right after compression the decompression returns correct results.
	 */
	@Getter
	@Setter
	protected static boolean checkPostCompression = DEFAULT_CHECK_POST_COMPRESSION;

}
