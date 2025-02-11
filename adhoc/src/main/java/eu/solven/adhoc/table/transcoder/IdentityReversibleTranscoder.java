/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.transcoder;

import java.util.Set;

import eu.solven.adhoc.table.IAdhocTableWrapper;

/**
 * Sometimes (e.g. in early projects) there is a direct mapping from columns used by
 * {@link eu.solven.adhoc.query.AdhocQuery} and those provided by a {@link IAdhocTableWrapper}. Then, the transcoding is
 * the identity.
 *
 * This always returns the input column, hence it is reversible.
 */
public class IdentityReversibleTranscoder implements IAdhocTableTranscoder, IAdhocTableReverseTranscoder {
	@Override
	public String underlying(String queried) {
		return queried;
	}

	@Override
	public Set<String> queried(String underlying) {
		return Set.of(underlying);
	}
}
