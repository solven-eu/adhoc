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
package eu.solven.adhoc.data.row.slice;

import java.util.ArrayList;
import java.util.List;

import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable {@link IAdhocSlice} to be compressed by pages. The compression algorithm is delegated to another class.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
@SuperBuilder
@Slf4j
public class ByPageSliceCompressor implements ISliceCompressor {

	/**
	 * Number of rows to accumulate in a page
	 */
	@Default
	final int pageSize = AdhocUnsafe.pageSize;

	/**
	 * Current page
	 */
	final List<ProxiedSlice> page = new ArrayList<>();

	@Override
	public IAdhocSlice compress(IAdhocSlice slice) {
		ProxiedSlice proxiedSlice = new ProxiedSlice(slice);

		page.add(proxiedSlice);

		if (isFull()) {
			compressPage();
		}

		return proxiedSlice;
	}

	protected void compressPage() {
		final List<ProxiedSlice> toCompress = new ArrayList<>(page);
		page.clear();

		doCompress(toCompress);

	}

	protected void doCompress(List<ProxiedSlice> toCompress) {
		log.info("Compressing a page with with={}", toCompress.size());
	}

	protected boolean isFull() {
		return page.size() >= pageSize;
	}

}
