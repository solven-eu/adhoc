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

import java.nio.charset.StandardCharsets;
import java.util.List;

import eu.solven.adhoc.encoding.bytes.IByteSlice;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.fsst.IFsstDecoder;
import lombok.Builder;
import lombok.NonNull;

/**
 * 
 * @author Benoit Lacelle
 */
@Builder
public class FsstReadableColumn implements IReadableColumn {

	@NonNull
	protected IFsstDecoder decoder;

	@NonNull
	protected List<IByteSlice> encoded;

	@Override
	public Object readValue(int rowIndex) {
		IByteSlice encodedBytes = encoded.get(rowIndex);

		if (encodedBytes == null) {
			return null;
		}

		IByteSlice decodedBytes = decoder.decodeAll(encodedBytes);
		return new String(decodedBytes.buffer(), decodedBytes.offset(), decodedBytes.length(), StandardCharsets.UTF_8);
	}

}