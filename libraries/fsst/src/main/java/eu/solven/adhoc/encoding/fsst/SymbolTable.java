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
package eu.solven.adhoc.encoding.fsst;

import com.google.errorprone.annotations.ThreadSafe;

import eu.solven.adhoc.encoding.bytes.IByteSlice;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents a stable table of symbols, enabling encoding and decoding.
 * 
 * @author Benoit Lacelle
 */
@ThreadSafe
@RequiredArgsConstructor
@SuppressWarnings("checkstyle:MagicNumber")
class SymbolTable implements IFsstConstants, IFsstEncoding {
	final SymbolTableEncoder encoder;

	@Getter
	final SymbolTableDecoder decoder;

	// Encode compresses input, reusing buf if provided.
	// Returns compressed data (may be a different slice than buf).
	@Override
	public IByteSlice encode(byte[] buf, IByteSlice input) {
		return encoder.encode(buf, input);
	}

	@Override
	public IByteSlice decode(byte[] buf, IByteSlice src) {
		return decoder.decode(buf, src);
	}

	@Override
	public IByteSlice decode(byte[] buf, byte[] src) {
		return decoder.decode(buf, src);
	}

	@Override
	public IByteSlice decode(byte[] buf, byte[] src, int srcStart, int srcEnd) {
		return decoder.decode(buf, src, srcStart, srcEnd);
	}

	@Override
	public IByteSlice decodeAll(byte[] src) {
		return decoder.decode(null, src);
	}

	@Override
	public IByteSlice decodeAll(IByteSlice src) {
		return decoder.decode(null, src);
	}

	@Override
	public IFsstDecoder asDecoder() {
		return decoder;
	}
}